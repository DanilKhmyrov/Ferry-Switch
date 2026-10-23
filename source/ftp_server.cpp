#include "ftp_server.hpp"

#include <switch.h>

#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <map>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

// Larger than a stack buffer would be comfortable at, so this is
// heap-allocated (once per transfer, reused across chunks) instead of
// per-chunk — fewer send()/recv() and fread()/fwrite() calls per megabyte
// than a smaller buffer would need.
constexpr size_t transferChunkSize = 256 * 1024;

// Content-storage reads (ncmContentStorageReadContentIdFile, used for
// virtual .nsp downloads under InstalledGames) go through the ncm service
// over IPC rather than a local, already-open file descriptor like fread()
// — each call almost certainly carries fixed per-call overhead on top of
// the actual read, which transferChunkSize's 256KB wasn't sized around
// (reported real-world throughput: ~2-3MB/s here vs. DBI's ~10-12MB/s
// reading the same content). A larger chunk amortizes that overhead over
// more bytes per call. Unverified without hardware to profile against —
// if this doesn't move the needle, the bottleneck is elsewhere.
constexpr size_t contentReadChunkSize = 4 * 1024 * 1024;

// stdio's default buffer is on the order of a kilobyte, so every 256KB
// fwrite() of received data was being chopped into hundreds of small
// underlying writes — each one a round trip to the filesystem service —
// while the download path writes straight to the socket with no stdio in
// the way at all. That asymmetry lines up exactly with uploads measuring
// ~3MB/s against ~9-10MB/s for downloads over the same connection. A
// buffer comfortably larger than transferChunkSize lets whole chunks go
// out in one write instead. Installed via setvbuf() in beginStorRecv(),
// backed by FtpClient::fileBuffer (setvbuf does not take ownership).
constexpr size_t uploadWriteBufferSize = 1 * 1024 * 1024;

// How many clients can be connected (browsing or transferring) at once.
// Sized a little above the Mac app's default of 3 parallel transfer
// workers so a browse-only connection doesn't starve them for a slot.
// Ferry's own macOS client needs four of these on its own: one control
// connection for browsing plus one per concurrent transfer, and it runs
// three at a time. At four this left no headroom whatsoever — a second
// client, or a worker from the previous transfer that hadn't been reaped
// yet, and the next connection had nowhere to go. Eight fits two of them.
constexpr size_t kMaxClients = 8;

// How long a PASV listener waits for the client to actually open the data
// connection before giving up — matches the original blocking version's
// select() timeout.
constexpr long long dataConnectTimeoutMs = 10000;

void setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

long long nowMs() {
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<long long>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

bool wouldBlock() {
    return errno == EWOULDBLOCK || errno == EAGAIN;
}

}

bool FtpServer::start(uint16_t port) {
    m_port = port;
    m_listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (m_listenFd < 0) {
        return false;
    }

    int yes = 1;
    setsockopt(m_listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(m_listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        return false;
    }
    if (listen(m_listenFd, static_cast<int>(kMaxClients)) != 0) {
        return false;
    }
    setNonBlocking(m_listenFd);

    refreshLocalIp();
    mutexInit(&m_lock);
    publishSnapshot();

    // Priority one step below the main thread's usual 0x2C (higher number
    // = lower priority), so the frame loop always wins when both are
    // runnable — a transfer saturating this thread must never cost frames.
    // Default core, 128KB stack: the deepest thing running here is the
    // ncm content scan's few-KB key array, everything large is heap.
    if (R_FAILED(threadCreate(&m_thread, &FtpServer::threadEntry, this, nullptr, 0x20000, 0x2D, -2))) {
        return false;
    }
    if (R_FAILED(threadStart(&m_thread))) {
        threadClose(&m_thread);
        return false;
    }
    m_threadStarted = true;
    return true;
}

void FtpServer::threadEntry(void* self) {
    static_cast<FtpServer*>(self)->threadMain();
}

void FtpServer::threadMain() {
    while (!m_stopRequested) {
        applyPendingRequests();
        bool didWork = poll();
        publishSnapshot();

        // Back-off in three steps, so an active transfer is never throttled
        // but an idle console isn't waking a core for nothing either:
        //   - moved data: yield only, come straight back for more
        //   - connected but quiet: ~500 checks/sec, still far more
        //     responsive than the 60 this got sharing the frame loop
        //   - nobody connected: one check per frame's worth of time is
        //     plenty to notice an accept()
        u64 idleNs = 0;
        if (!didWork) {
            idleNs = m_clients.empty() ? 16'000'000ULL : 2'000'000ULL;
        }
        svcSleepThread(idleNs);
    }
}

void FtpServer::applyPendingRequests() {
    mutexLock(&m_lock);
    bool wantDisconnect = m_disconnectRequested;
    bool wantPin = m_pinRequested;
    std::string newPin = m_requestedPin;
    bool wantRoot = m_rootRequested;
    FtpRoot newRoot = m_requestedRoot;
    m_disconnectRequested = false;
    m_pinRequested = false;
    m_rootRequested = false;
    mutexUnlock(&m_lock);

    // Disconnect first: setRoot() refuses while anyone is still connected,
    // so asking for both in one go (the natural "kick everyone off and
    // switch" gesture) has to apply in this order to work.
    if (wantDisconnect) {
        disconnectAll();
        m_clients.erase(
            std::remove_if(m_clients.begin(), m_clients.end(), [](const FtpClient& c) { return c.disconnected; }),
            m_clients.end());
    }
    if (wantPin) {
        m_pin = newPin;
    }
    if (wantRoot) {
        setRoot(newRoot);
    }
}

void FtpServer::publishSnapshot() {
    // ~30Hz is plenty for something only ever read by the renderer, and
    // this loop can spin thousands of times a second.
    long long now = nowMs();
    if (now - m_lastSnapshotMs < 33) {
        return;
    }
    m_lastSnapshotMs = now;

    ServerSnapshot fresh;
    fresh.localIp = m_localIp;
    fresh.clientCount = m_clients.size();
    fresh.primaryClientIp = primaryClientIp();
    fresh.transfers = activeTransfers();
    fresh.log = m_log;
    fresh.stats = m_stats;
    fresh.pinProtected = !m_pin.empty();
    fresh.root = m_root;

    mutexLock(&m_lock);
    m_snapshot = std::move(fresh);
    mutexUnlock(&m_lock);
}

ServerSnapshot FtpServer::snapshot() const {
    mutexLock(&m_lock);
    ServerSnapshot copy = m_snapshot;
    mutexUnlock(&m_lock);
    return copy;
}

void FtpServer::requestRoot(FtpRoot root) {
    mutexLock(&m_lock);
    m_rootRequested = true;
    m_requestedRoot = root;
    mutexUnlock(&m_lock);
}

void FtpServer::requestDisconnectAll() {
    mutexLock(&m_lock);
    m_disconnectRequested = true;
    mutexUnlock(&m_lock);
}

void FtpServer::setPin(const std::string& pin) {
    mutexLock(&m_lock);
    m_pinRequested = true;
    m_requestedPin = pin;
    mutexUnlock(&m_lock);
}

void FtpServer::refreshLocalIp() {
    // gethostid() wraps nifm's "current IP address" query, which can be
    // stale for a moment right after boot if the network interface hasn't
    // finished acquiring its DHCP lease yet — refreshed periodically
    // rather than cached once so the address shown always matches reality.
    u32 ip = gethostid();
    in_addr localAddr{};
    localAddr.s_addr = ip;
    m_localIp = inet_ntoa(localAddr);
    m_lastIpRefreshMs = nowMs();
}


void FtpServer::stop() {
    // Join before touching anything else — every member below is owned by
    // the server thread while it's alive.
    if (m_threadStarted) {
        m_stopRequested = true;
        threadWaitForExit(&m_thread);
        threadClose(&m_thread);
        m_threadStarted = false;
    }
    for (auto& client : m_clients) {
        closeClient(client);
    }
    m_clients.clear();
    if (m_listenFd >= 0) {
        close(m_listenFd);
        m_listenFd = -1;
    }
    if (m_nandMounted) {
        fsdevUnmountDevice("nand");
        m_nandMounted = false;
    }
}

bool FtpServer::setRoot(FtpRoot newRoot) {
    if (!m_clients.empty() || newRoot == m_root) {
        return newRoot == m_root;
    }

    if (m_nandMounted) {
        fsdevUnmountDevice("nand");
        m_nandMounted = false;
    }

    switch (newRoot) {
        case FtpRoot::Sd:
            m_root = FtpRoot::Sd;
            m_rootPrefix = "sdmc:";
            break;
        case FtpRoot::Nand: {
            FsFileSystem fs;
            if (R_FAILED(fsOpenBisFileSystem(&fs, FsBisPartitionId_User, ""))) {
                logEvent(LogEntry::Kind::Error, "Could not open NAND");
                return false;
            }
            if (fsdevMountDevice("nand", fs) < 0) {
                logEvent(LogEntry::Kind::Error, "Could not mount NAND");
                return false;
            }
            m_nandMounted = true;
            m_root = FtpRoot::Nand;
            m_rootPrefix = "nand:";
            break;
        }
        case FtpRoot::InstalledGames:
            m_root = FtpRoot::InstalledGames;
            m_rootPrefix.clear(); // synthetic — never used directly at the top level
            break;
        case FtpRoot::Saves:
            m_root = FtpRoot::Saves;
            m_rootPrefix.clear();
            break;
    }
    return true;
}

void FtpServer::disconnectAll() {
    // closeClient() itself is idempotent and just flips client.disconnected
    // — poll()'s sweep (already running every frame) does the actual
    // removal from m_clients right after, same as any other disconnect.
    for (auto& client : m_clients) {
        closeClient(client);
    }
}

// Lazily loaded once per session (not once per listing) — titles don't
// change while Ferry is running, and each load is a bounded but real
// blocking cost (one IPC round-trip per installed title).
const std::vector<TitleInfo>& FtpServer::titles() {
    if (!m_titleCacheLoaded) {
        m_titleCache = listInstalledTitles();
        m_titleCacheLoaded = true;
    }
    return m_titleCache;
}

bool FtpServer::atVirtualTop(const FtpClient& client) const {
    if (m_root != FtpRoot::InstalledGames && m_root != FtpRoot::Saves) {
        return false;
    }
    return client.cwd == "/" && !client.virtualMounted && client.virtualTitleId == 0 &&
           !client.installsEntered;
}

bool FtpServer::isReadOnly(const FtpClient& client) const {
    if (m_root == FtpRoot::Sd) {
        return false;
    }
    if (m_root == FtpRoot::Saves && client.virtualMounted) {
        return false; // a real, writable mount from here on — see enterTitle()
    }
    if (m_root == FtpRoot::InstalledGames && client.installsEntered) {
        return false; // Ferry's own real upload-staging folder, not synthetic — see localPath()
    }
    return true;
}

bool FtpServer::canStor(const FtpClient& client) const {
    if (!isReadOnly(client)) {
        return true;
    }
    return m_root == FtpRoot::InstalledGames && atVirtualTop(client);
}

void FtpServer::commitIfSaveMounted(FtpClient& client) {
    if (m_root == FtpRoot::Saves && client.virtualMounted) {
        fsdevCommitDevice(client.virtualMountName.c_str());
    }
}

namespace {

// One content item as it will appear inside the virtual .nsp — internal
// PFS0 entry name (matching real Nintendo NSPs: lowercase content-id hex,
// ".cnmt.nca" for the meta/cnmt entry, ".nca" for everything else), the
// content id used to actually read it, its size, and which storage it has
// to be read from (see RetrContentItem).
struct NspContentItem {
    std::string internalName;
    NcmContentId id;
    s64 size;
    NcmStorageId storageId;
};

std::vector<NspContentItem> listNspContentItems(NcmContentMetaDatabase& db, const NcmContentMetaKey& key,
                                                 NcmStorageId storageId) {
    std::vector<NspContentItem> items;
    NcmContentInfo infos[16];
    s32 infoWritten = 0;
    if (R_FAILED(ncmContentMetaDatabaseListContentInfo(&db, &infoWritten, infos, 16, &key, 0))) {
        return items;
    }
    for (s32 i = 0; i < infoWritten; i++) {
        const NcmContentInfo& info = infos[i];
        char idHex[40];
        for (int b = 0; b < 16; b++) {
            std::snprintf(idHex + b * 2, 3, "%02x", info.content_id.c[b]);
        }
        std::string name = std::string(idHex) + (info.content_type == NcmContentType_Meta ? ".cnmt.nca" : ".nca");
        s64 size = static_cast<s64>(static_cast<u64>(info.size_low) | (static_cast<u64>(info.size_high) << 32));
        items.push_back(NspContentItem{ name, info.content_id, size, storageId });
    }
    return items;
}

// Opens/queries/closes an NcmContentMetaDatabase per meta rather than
// holding one open across the whole call — metas being merged together
// (the "[B+U]" file) can each live on a different storage, so there's no
// single handle that would work for both anyway, and this keeps every
// caller (a single Base/Update file, or a merged one) going through the
// same path instead of a single-meta fast case plus a separate multi-meta
// one.
std::vector<NspContentItem> gatherItems(const std::vector<VirtualContentEntry>& metas) {
    std::vector<NspContentItem> items;
    for (const auto& meta : metas) {
        NcmContentMetaDatabase db;
        if (R_FAILED(ncmOpenContentMetaDatabase(&db, meta.storageId))) {
            continue;
        }
        std::vector<NspContentItem> part = listNspContentItems(db, meta.key, meta.storageId);
        ncmContentMetaDatabaseClose(&db);
        items.insert(items.end(), part.begin(), part.end());
    }
    return items;
}

s64 pfs0TotalSize(const std::vector<NspContentItem>& items) {
    size_t stringTableSize = 0;
    s64 dataSize = 0;
    for (const auto& item : items) {
        stringTableSize += item.internalName.size() + 1;
        dataSize += item.size;
    }
    s64 headerSize = static_cast<s64>(16 + 24 * items.size() + stringTableSize);
    return headerSize + dataSize;
}

// Which meta (if any) backs a title's base game and its update, on
// whichever storage each was actually found on — computed once per
// listing request (top-level or per-title) rather than once per title,
// since ncmContentMetaDatabaseListApplication() already returns every
// title's entry for a given type+storage in one call.
struct AppMetas {
    std::optional<VirtualContentEntry> base;
    std::optional<VirtualContentEntry> update;
};

std::map<u64, AppMetas> scanInstalledMetas() {
    std::map<u64, AppMetas> result;
    struct Group { NcmContentMetaType type; bool isUpdate; };
    const Group groups[] = {
        { NcmContentMetaType_Application, false },
        { NcmContentMetaType_Patch, true },
    };
    const NcmStorageId storages[] = { NcmStorageId_BuiltInUser, NcmStorageId_SdCard };

    for (NcmStorageId storageId : storages) {
        NcmContentMetaDatabase db;
        if (R_FAILED(ncmOpenContentMetaDatabase(&db, storageId))) {
            continue;
        }
        for (const auto& group : groups) {
            NcmApplicationContentMetaKey keys[128];
            s32 total = 0;
            s32 written = 0;
            if (R_FAILED(ncmContentMetaDatabaseListApplication(&db, &total, &written, keys, 128, group.type))) {
                continue;
            }
            for (s32 i = 0; i < written; i++) {
                VirtualContentEntry entry{ keys[i].key, storageId };
                AppMetas& metas = result[keys[i].application_id];
                (group.isUpdate ? metas.update : metas.base) = entry;
            }
        }
        ncmContentMetaDatabaseClose(&db);
    }
    return result;
}

// PFS0 (the container format .nsp files use): a 16-byte header ("PFS0" +
// file count + string-table size + reserved), one 24-byte entry per file
// (data offset relative to the start of the data section, size, name
// offset into the string table, reserved), the null-terminated name
// string table, then every file's raw bytes back to back starting right
// after the string table. No libnx helper builds this — it's assembled
// by hand here from the sizes/ids already queried from ncm. This is the
// least-tested part of the whole app: correct against the documented
// format on paper, never verified by actually installing a produced .nsp
// on real hardware.
std::string buildPfs0Header(const std::vector<NspContentItem>& items, s64& outDataSize) {
    std::string stringTable;
    std::vector<u32> nameOffsets;
    nameOffsets.reserve(items.size());
    for (const auto& item : items) {
        nameOffsets.push_back(static_cast<u32>(stringTable.size()));
        stringTable += item.internalName;
        stringTable += '\0';
    }

    u32 numFiles = static_cast<u32>(items.size());
    u32 stringTableSize = static_cast<u32>(stringTable.size());

    std::string header(16 + 24 * static_cast<size_t>(numFiles), '\0');
    std::memcpy(&header[0], "PFS0", 4);
    std::memcpy(&header[4], &numFiles, sizeof(numFiles));
    std::memcpy(&header[8], &stringTableSize, sizeof(stringTableSize));

    s64 dataOffset = 0;
    for (u32 i = 0; i < numFiles; i++) {
        size_t base = 16 + 24 * static_cast<size_t>(i);
        u64 off = static_cast<u64>(dataOffset);
        u64 sz = static_cast<u64>(items[i].size);
        u32 nameOff = nameOffsets[i];
        std::memcpy(&header[base], &off, sizeof(off));
        std::memcpy(&header[base + 8], &sz, sizeof(sz));
        std::memcpy(&header[base + 16], &nameOff, sizeof(nameOff));
        dataOffset += items[i].size;
    }
    header += stringTable;
    outDataSize = dataOffset;
    return header;
}

}

// Top-level InstalledGames/Saves listing — one folder per installed title,
// plus (InstalledGames only) a merged "Title[B+U<version>].nsp" file for
// any title that has both a base game and an update installed, combining
// both metas' content into one PFS0 — the same convenience file DBI
// itself offers at this level alongside the per-title folders. Saves has
// no such thing (there's no content to merge, just save data), so that
// part is skipped entirely for that root.
std::string FtpServer::buildTitleListing(FtpClient& client) const {
    client.virtualContentFiles.clear();
    std::string listing;

    if (m_root == FtpRoot::InstalledGames) {
        listing += "drwxr-xr-x 1 switch switch 0 Jan  1 1970 Installs\r\n";
    }

    for (const auto& title : m_titleCache) {
        char line[600];
        std::snprintf(line, sizeof(line), "drwxr-xr-x 1 switch switch 0 Jan  1 1970 %s\r\n",
                      title.displayName.c_str());
        listing += line;
    }

    if (m_root != FtpRoot::InstalledGames) {
        return listing;
    }

    std::map<u64, AppMetas> metas = scanInstalledMetas();
    for (const auto& title : m_titleCache) {
        auto it = metas.find(title.applicationId);
        if (it == metas.end() || !it->second.base || !it->second.update) {
            continue; // no update installed -- nothing to merge
        }

        std::vector<VirtualContentEntry> combo{ *it->second.base, *it->second.update };
        std::vector<NspContentItem> items = gatherItems(combo);
        if (items.empty()) {
            continue;
        }

        std::string filename =
            title.displayName + "[B+U" + std::to_string(it->second.update->key.version) + "].nsp";
        char line[768];
        std::snprintf(line, sizeof(line), "-rw-r--r-- 1 switch switch %lld Jan  1 1970 %s\r\n",
                      static_cast<long long>(pfs0TotalSize(items)), filename.c_str());
        listing += line;

        client.virtualContentFiles.push_back({ filename, std::move(combo) });
    }

    return listing;
}

// Lists this title's installed content as real, complete .nsp files — one
// for the base game, one for an installed update if present — matching
// what DBI itself shows, rather than loose raw .nca fragments. Naming
// matches DBI's own convention: "Title[vVERSION][Base].nsp" /
// "Title[vVERSION][Update].nsp".
//
// Checks both NcmStorageId_BuiltInUser and NcmStorageId_SdCard via
// scanInstalledMetas() — a title (and even a title's base game vs. its
// update) can be installed to either, and a content-meta database handle
// only sees whichever one it was opened against, so querying just one
// silently misses anything installed to the other. That, plus an earlier
// version relying on ncmContentMetaDatabaseList()'s unconfirmed
// id/id_min/id_max filtering, and — the actual root cause behind both —
// ncmInitialize() never having been called at all, were three real,
// separate bugs this listing coming back empty turned out to trace to.
//
// Any ncm failure yields an empty listing (a "no content found" folder)
// rather than propagating an error.
std::string FtpServer::buildTitleContentListing(FtpClient& client) const {
    client.virtualContentFiles.clear();
    std::string listing;

    std::string titleName;
    for (const auto& title : m_titleCache) {
        if (title.applicationId == client.virtualTitleId) {
            titleName = title.displayName;
            break;
        }
    }
    if (titleName.empty()) {
        char idHex[20];
        std::snprintf(idHex, sizeof(idHex), "%016llX", static_cast<unsigned long long>(client.virtualTitleId));
        titleName = idHex;
    }

    std::map<u64, AppMetas> metas = scanInstalledMetas();
    auto it = metas.find(client.virtualTitleId);
    if (it == metas.end()) {
        return listing;
    }

    struct Slot { const std::optional<VirtualContentEntry>* entry; const char* tag; };
    const Slot slots[] = {
        { &it->second.base, "Base" },
        { &it->second.update, "Update" },
    };

    for (const auto& slot : slots) {
        if (!*slot.entry) {
            continue;
        }
        std::vector<VirtualContentEntry> single{ **slot.entry };
        std::vector<NspContentItem> items = gatherItems(single);
        if (items.empty()) {
            continue;
        }

        std::string filename =
            titleName + "[v" + std::to_string((*slot.entry)->key.version) + "][" + slot.tag + "].nsp";
        char line[768];
        std::snprintf(line, sizeof(line), "-rw-r--r-- 1 switch switch %lld Jan  1 1970 %s\r\n",
                      static_cast<long long>(pfs0TotalSize(items)), filename.c_str());
        listing += line;

        client.virtualContentFiles.push_back({ filename, std::move(single) });
    }

    return listing;
}

// Resolves `folderName` (as typed in a CWD from the virtual top level)
// against the title cache and, depending on the active root, either
// mounts that title's save data (Saves) or switches to its synthetic
// content listing (InstalledGames). Returns false (leaving `client`
// unchanged) if the name doesn't match a known title or the mount fails.
bool FtpServer::enterTitle(FtpClient& client, const std::string& folderName) {
    // Through titles() rather than m_titleCache directly: the cache is
    // filled on first use, and a client can reach a CWD before anything has
    // listed the top level — curl walks straight to the folder it was given
    // in the URL. Reading the member here meant that first CWD searched an
    // empty cache and answered "550 Directory not found" for a title that
    // is installed.
    const TitleInfo* match = nullptr;
    for (const auto& title : titles()) {
        if (title.displayName == folderName) {
            match = &title;
            break;
        }
    }
    if (!match) {
        return false;
    }

    if (m_root == FtpRoot::Saves) {
        AccountUid uid{};
        bool haveUid = R_SUCCEEDED(accountGetPreselectedUser(&uid));
        if (!haveUid) {
            haveUid = R_SUCCEEDED(accountGetLastOpenedUser(&uid));
        }
        if (!haveUid) {
            AccountUid uids[8];
            s32 count = 0;
            if (R_SUCCEEDED(accountListAllUsers(uids, 8, &count)) && count > 0) {
                uid = uids[0];
                haveUid = true;
            }
        }
        if (!haveUid) {
            // Logged rather than just returning false — a save that fails
            // to mount looks identical to a client-side navigation glitch
            // from the outside otherwise, and this class of "silently
            // swallowed failure looked like a different bug" has bitten
            // this codebase more than once already.
            logEvent(LogEntry::Kind::Error, "Save enter: no account UID resolved for " + folderName);
            return false;
        }

        // Mounted read-write (not fsdevMountSaveDataReadOnly) so save
        // restores/edits actually work — every command that could reach
        // here besides SD is still blocked by isReadOnly()/canStor(), so
        // this is the one deliberate exception, not a gap.
        std::string mountName = "save" + std::to_string(client.slotId);
        Result mountRc = fsdevMountSaveData(mountName.c_str(), match->applicationId, uid);
        if (R_FAILED(mountRc)) {
            char buf[96];
            std::snprintf(buf, sizeof(buf), "Save mount failed for %s (0x%x)", folderName.c_str(), mountRc);
            logEvent(LogEntry::Kind::Error, buf);
            return false;
        }
        client.virtualMounted = true;
        client.virtualMountName = mountName;
        client.cwd = "/";
        client.virtualCwdDisplay = "/" + folderName + "/";
        return true;
    }

    // InstalledGames: no real mount — just switch this client into that
    // title's synthetic content listing.
    client.virtualTitleId = match->applicationId;
    client.cwd = "/";
    client.virtualCwdDisplay = "/" + folderName + "/";
    return true;
}

void FtpServer::leaveTitle(FtpClient& client) {
    if (client.virtualMounted) {
        fsdevUnmountDevice(client.virtualMountName.c_str());
        client.virtualMounted = false;
        client.virtualMountName.clear();
    }
    client.virtualTitleId = 0;
    client.virtualContentFiles.clear();
    client.cwd = "/";
    client.virtualCwdDisplay.clear();
}

void FtpServer::unmountClientVirtual(FtpClient& client) {
    if (client.virtualMounted) {
        fsdevUnmountDevice(client.virtualMountName.c_str());
        client.virtualMounted = false;
        client.virtualMountName.clear();
    }
    if (client.retrFromContent) {
        // May never have actually been opened — see beginContentRetr(),
        // which no longer opens it eagerly since a merged file's storage
        // isn't known until the first item is actually read.
        if (client.retrContentStorageId != NcmStorageId_None) {
            ncmContentStorageClose(&client.retrContentStorage);
            client.retrContentStorageId = NcmStorageId_None;
        }
        client.retrFromContent = false;
    }
}

bool FtpServer::poll() {
    if (nowMs() - m_lastIpRefreshMs > 3000) {
        refreshLocalIp();
    }

    acceptNewClients();

    bool didWork = false;
    for (auto& client : m_clients) {
        if (serviceClient(client)) {
            didWork = true;
        }
    }

    m_clients.erase(
        std::remove_if(m_clients.begin(), m_clients.end(), [](const FtpClient& c) { return c.disconnected; }),
        m_clients.end());
    return didWork;
}

void FtpServer::acceptNewClients() {
    for (;;) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int fd = accept(m_listenFd, reinterpret_cast<sockaddr*>(&addr), &len);
        if (fd < 0) {
            return;
        }
        setNonBlocking(fd);

        std::string ip = inet_ntoa(addr.sin_addr);

        // Being full used to mean simply not calling accept() — but the
        // kernel completes the handshake into the listen backlog anyway, so
        // the client believed it had connected and sat waiting for a 220
        // that was never coming. A silent hang, with no error to show and
        // nothing in the log. Say so and hang up instead, and keep draining
        // so nobody else is left waiting in the backlog either.
        if (m_clients.size() >= kMaxClients) {
            static const char kFull[] = "421 Too many connections\r\n";
            send(fd, kFull, sizeof(kFull) - 1, 0);
            close(fd);
            logEvent(LogEntry::Kind::Error, "Refused " + ip + ": all " +
                                                std::to_string(kMaxClients) + " slots in use");
            continue;
        }

        FtpClient client;
        client.controlFd = fd;
        client.ip = ip;
        client.awaitingPassword = !m_pin.empty();
        client.slotId = m_nextClientId++;
        m_clients.push_back(std::move(client));

        reply(m_clients.back(), "220 Ferry ready\r\n");
        logEvent(LogEntry::Kind::Connected, ip);
    }
}

bool FtpServer::serviceClient(FtpClient& client) {
    if (client.op == FtpClient::Op::AwaitData) {
        if (!acceptClientData(client)) {
            if (nowMs() > client.dataWaitDeadlineMs) {
                reply(client, "425 Can't open data connection\r\n");
                client.op = FtpClient::Op::Idle;
                client.pendingKind = FtpClient::PendingKind::None;
            }
            return false;
        }
        switch (client.pendingKind) {
            case FtpClient::PendingKind::List:
                beginListSend(client);
                break;
            case FtpClient::PendingKind::Retr:
                beginRetrSend(client, client.pendingName);
                break;
            case FtpClient::PendingKind::Stor:
                beginStorRecv(client, client.pendingName);
                break;
            case FtpClient::PendingKind::None:
                break;
        }
        client.pendingKind = FtpClient::PendingKind::None;
        client.pendingName.clear();
        return true;
    }

    if (client.op == FtpClient::Op::ListSend || client.op == FtpClient::Op::RetrSend ||
        client.op == FtpClient::Op::StorRecv) {
        return stepTransfer(client);
    }

    char buf[512];
    ssize_t n = recv(client.controlFd, buf, sizeof(buf), 0);
    if (n > 0) {
        client.recvBuffer.append(buf, static_cast<size_t>(n));
        processBuffered(client);
        return true;
    } else if (n == 0) {
        closeClient(client);
    } else if (!wouldBlock()) {
        closeClient(client);
    }
    return false;
}

void FtpServer::processBuffered(FtpClient& client) {
    size_t pos;
    while (!client.disconnected && (pos = client.recvBuffer.find('\n')) != std::string::npos) {
        std::string line = client.recvBuffer.substr(0, pos);
        client.recvBuffer.erase(0, pos + 1);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            handleCommand(client, line);
        }
    }
}

void FtpServer::handleCommand(FtpClient& client, const std::string& line) {
    std::string cmd;
    std::string arg;
    size_t sp = line.find(' ');
    cmd = (sp == std::string::npos) ? line : line.substr(0, sp);
    arg = (sp == std::string::npos) ? "" : line.substr(sp + 1);
    for (auto& c : cmd) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }

    if (cmd == "USER") {
        reply(client, m_pin.empty() ? "230 User logged in\r\n" : "331 Password required\r\n");
        return;
    }
    if (cmd == "PASS") {
        if (m_pin.empty() || arg == m_pin) {
            client.awaitingPassword = false;
            reply(client, "230 Logged in\r\n");
        } else {
            reply(client, "530 Login incorrect\r\n");
            closeClient(client);
        }
        return;
    }
    if (cmd == "QUIT") {
        reply(client, "221 Bye\r\n");
        closeClient(client);
        return;
    }
    if (client.awaitingPassword) {
        reply(client, "530 Please login with USER and PASS\r\n");
        return;
    }

    if (cmd == "SYST") {
        reply(client, "215 UNIX Type: L8\r\n");
    } else if (cmd == "TYPE") {
        reply(client, "200 Type set\r\n");
    } else if (cmd == "ALLO") {
        // No real pre-allocation — just remembers the announced size so
        // the *next* STOR's progress display has a total/percentage to
        // show instead of only "bytes received so far". A malformed or
        // missing argument just means nothing gets remembered, not an
        // error — ALLO is inherently advisory. strtoll rather than
        // std::stoll since this codebase builds with -fno-exceptions.
        char* end = nullptr;
        long long parsed = std::strtoll(arg.c_str(), &end, 10);
        client.pendingAlloBytes = (end != arg.c_str() && parsed > 0) ? parsed : -1;
        reply(client, "202 No storage allocation necessary\r\n");
    } else if (cmd == "PWD" || cmd == "XPWD") {
        reply(client, "257 \"" + reportedCwd(client) + "\"\r\n");
    } else if (cmd == "CWD" || cmd == "XCWD") {
        changeDirectory(client, arg);
    } else if (cmd == "CDUP" || cmd == "XCUP") {
        changeDirectory(client, "..");
    } else if (cmd == "PASV") {
        enterPassive(client);
    } else if (cmd == "LIST" || cmd == "NLST") {
        beginAwaitData(client, FtpClient::PendingKind::List, "");
    } else if (cmd == "RETR") {
        beginAwaitData(client, FtpClient::PendingKind::Retr, arg);
    } else if (cmd == "STOR") {
        // Every command that can modify the filesystem is gated through
        // this single check (or canStor()'s slightly wider version of it)
        // — one chokepoint is much easier to audit than a guard scattered
        // into each doXxx() handler, and this is the one thing standing
        // between a stray write and the console's own NAND/save data when
        // a non-Sd root is active.
        if (!canStor(client)) {
            reply(client, "550 Read-only filesystem\r\n");
        } else {
            beginAwaitData(client, FtpClient::PendingKind::Stor, arg);
        }
    } else if (cmd == "MKD" || cmd == "XMKD") {
        if (isReadOnly(client)) { reply(client, "550 Read-only filesystem\r\n"); } else { doMkd(client, arg); }
    } else if (cmd == "RMD" || cmd == "XRMD") {
        if (isReadOnly(client)) { reply(client, "550 Read-only filesystem\r\n"); } else { doRmd(client, arg); }
    } else if (cmd == "DELE") {
        if (isReadOnly(client)) { reply(client, "550 Read-only filesystem\r\n"); } else { doDele(client, arg); }
    } else if (cmd == "RNFR") {
        if (isReadOnly(client)) { reply(client, "550 Read-only filesystem\r\n"); } else { doRnfr(client, arg); }
    } else if (cmd == "RNTO") {
        if (isReadOnly(client)) { reply(client, "550 Read-only filesystem\r\n"); } else { doRnto(client, arg); }
    } else if (cmd == "SIZE") {
        doSize(client, arg);
    } else if (cmd == "NOOP") {
        reply(client, "200 NOOP\r\n");
    } else {
        reply(client, "502 Command not implemented\r\n");
    }
}

void FtpServer::reply(FtpClient& client, const std::string& text) {
    if (client.controlFd < 0) {
        return;
    }
    const char* p = text.data();
    size_t sent = 0;
    while (sent < text.size()) {
        ssize_t n = send(client.controlFd, p + sent, text.size() - sent, 0);
        if (n <= 0) {
            return;
        }
        sent += static_cast<size_t>(n);
    }
}

std::string FtpServer::reportedCwd(const FtpClient& client) const {
    if (client.virtualCwdDisplay.empty()) {
        return client.cwd;
    }
    // client.cwd always starts with "/" and virtualCwdDisplay always ends
    // with one, so drop cwd's leading slash to avoid a doubled "//".
    return client.virtualCwdDisplay + client.cwd.substr(1);
}

std::string FtpServer::localPath(const FtpClient& client, const std::string& name) const {
    std::string prefix = m_rootPrefix;
    if (m_root == FtpRoot::Saves && client.virtualMounted) {
        prefix = client.virtualMountName + ":";
    } else if (m_root == FtpRoot::InstalledGames && client.installsEntered) {
        prefix = "sdmc:/switch/Ferry/Installs";
    }
    std::string base = prefix + client.cwd;
    if (name.empty()) {
        return base;
    }
    if (name[0] == '/') {
        if (prefix.empty()) {
            return "";
        }
        if (prefix.back() == '/') {
            return prefix + name.substr(1);
        }
        return prefix + name;
    }
    if (!base.empty() && base.back() != '/') {
        base += "/";
    }
    return base + name;
}

void FtpServer::changeDirectory(FtpClient& client, const std::string& arg) {
    // Virtual roots: at the synthetic top level, CWD understands ".."
    // (stay put — there's nothing above it), a title's exact display name
    // (enters it), or — InstalledGames only — "Installs" (enters Ferry's
    // own real upload-staging folder, see beginStorRecv). Inside a title,
    // ".." leaves it (unmounting a save if one was mounted);
    // InstalledGames' per-title content listing is flat, so nothing else
    // is navigable there.
    if (m_root == FtpRoot::InstalledGames || m_root == FtpRoot::Saves) {
        // An absolute path has to be resolved from the virtual top rather
        // than falling through to the real-path logic below, which knows
        // nothing about titles. This is not a rare case: every transfer
        // the Mac client runs opens its own connection and navigates it
        // with a single absolute CWD to the directory it captured from
        // PWD (e.g. "/Terraria [0100E46006708000]/"), so without this,
        // downloading anything at all from InstalledGames or Saves fails
        // with 550 before it starts — only the browsing connection, which
        // got there by relative steps, could see those folders.
        if (!arg.empty() && arg[0] == '/' && arg != "/") {
            std::vector<std::string> segments;
            size_t start = 0;
            while (start < arg.size()) {
                size_t slash = arg.find('/', start);
                if (slash == std::string::npos) {
                    slash = arg.size();
                }
                if (slash > start) {
                    segments.push_back(arg.substr(start, slash - start));
                }
                start = slash + 1;
            }

            // Unwind whatever this client is currently inside, so the path
            // is resolved from the top exactly as written.
            if (client.virtualMounted || client.virtualTitleId != 0) {
                leaveTitle(client);
            }
            client.installsEntered = false;
            client.virtualCwdDisplay.clear();
            client.cwd = "/";

            if (segments.empty()) {
                reply(client, "250 Directory changed\r\n");
                return;
            }

            bool entered = false;
            if (m_root == FtpRoot::InstalledGames && segments[0] == "Installs") {
                client.installsEntered = true;
                client.virtualCwdDisplay = "/Installs/";
                entered = true;
            } else {
                entered = enterTitle(client, segments[0]);
            }
            if (!entered) {
                reply(client, "550 Directory not found\r\n");
                return;
            }

            // Anything past the first segment is a real path inside a
            // mounted save or the Installs folder; InstalledGames' own
            // per-title listing is flat, so a deeper path there is bogus.
            if (segments.size() > 1) {
                if (m_root == FtpRoot::InstalledGames && !client.installsEntered) {
                    reply(client, "550 Directory not found\r\n");
                    return;
                }
                std::string deeper = "/";
                for (size_t i = 1; i < segments.size(); i++) {
                    deeper += segments[i] + "/";
                }
                // Set it first so localPath() resolves against the right
                // mount prefix, and put it back if the path isn't real.
                client.cwd = deeper;
                struct stat st{};
                if (stat(localPath(client, "").c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
                    client.cwd = "/";
                    reply(client, "550 Directory not found\r\n");
                    return;
                }
            }
            reply(client, "250 Directory changed\r\n");
            return;
        }
        if (atVirtualTop(client)) {
            if (arg == ".." || arg == "/") {
                reply(client, "250 Directory changed\r\n");
                return;
            }
            if (m_root == FtpRoot::InstalledGames && arg == "Installs") {
                client.installsEntered = true;
                client.cwd = "/";
                client.virtualCwdDisplay = "/Installs/";
                reply(client, "250 Directory changed\r\n");
                return;
            }
            if (enterTitle(client, arg)) {
                reply(client, "250 Directory changed\r\n");
            } else {
                reply(client, "550 Directory not found\r\n");
            }
            return;
        }
        if (m_root == FtpRoot::InstalledGames && client.virtualTitleId != 0) {
            // "/" as well as ".." — the Mac client's breadcrumb "Root"
            // segment sends an absolute CWD /, not "..", and InstalledGames'
            // content listing is flat (nothing to descend into first), so
            // either one unambiguously means "leave this title".
            if (arg == ".." || arg == "/") {
                leaveTitle(client);
                reply(client, "250 Directory changed\r\n");
            } else {
                reply(client, "550 Directory not found\r\n");
            }
            return;
        }
        // Unlike the flat InstalledGames case above, a mounted save (or
        // Installs) can have real subfolders, so ".." only unwinds one
        // real level at a time via the generic path logic below — except
        // right at the mount's own root, where it (and an absolute CWD /,
        // same breadcrumb reasoning as above — this one previously didn't
        // check for it at all, leaving the mount silently still entered)
        // means actually leaving.
        if (m_root == FtpRoot::Saves && client.virtualMounted &&
            (arg == "/" || (arg == ".." && client.cwd == "/"))) {
            leaveTitle(client);
            reply(client, "250 Directory changed\r\n");
            return;
        }
        if (m_root == FtpRoot::InstalledGames && client.installsEntered &&
            (arg == "/" || (arg == ".." && client.cwd == "/"))) {
            client.installsEntered = false;
            client.virtualCwdDisplay.clear();
            reply(client, "250 Directory changed\r\n");
            return;
        }
        // Otherwise this is real navigation inside an already-mounted
        // save, or inside Installs — falls through to the normal
        // path-based logic below, which localPath() resolves against the
        // right real prefix either way.
    }

    std::string newPath;
    if (arg == "..") {
        if (client.cwd != "/") {
            size_t end = client.cwd.size() >= 2 ? client.cwd.size() - 2 : 0;
            size_t pos = client.cwd.find_last_of('/', end);
            newPath = (pos == std::string::npos) ? "/" : client.cwd.substr(0, pos + 1);
            if (newPath.empty()) {
                newPath = "/";
            }
        } else {
            newPath = "/";
        }
    } else if (!arg.empty() && arg[0] == '/') {
        newPath = arg;
        if (newPath.back() != '/') {
            newPath += "/";
        }
    } else {
        newPath = client.cwd;
        if (!newPath.empty() && newPath.back() != '/') {
            newPath += "/";
        }
        newPath += arg + "/";
    }

    std::string prefix = (m_root == FtpRoot::Saves && client.virtualMounted)
                              ? (client.virtualMountName + ":")
                              : m_rootPrefix;
    std::string local = prefix + newPath;
    struct stat st{};
    if (stat(local.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        client.cwd = newPath;
        reply(client, "250 Directory changed\r\n");
    } else {
        reply(client, "550 Directory not found\r\n");
    }
}

void FtpServer::enterPassive(FtpClient& client) {
    closeClientData(client);
    client.pasvFd = socket(AF_INET, SOCK_STREAM, 0);
    setNonBlocking(client.pasvFd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;
    bind(client.pasvFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    listen(client.pasvFd, 1);

    socklen_t len = sizeof(addr);
    getsockname(client.pasvFd, reinterpret_cast<sockaddr*>(&addr), &len);
    uint16_t dataPort = ntohs(addr.sin_port);

    u32 ip = gethostid();
    const uint8_t* ipBytes = reinterpret_cast<const uint8_t*>(&ip);

    char buf[80];
    std::snprintf(buf, sizeof(buf), "227 Entering Passive Mode (%d,%d,%d,%d,%d,%d).\r\n",
                  ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3],
                  (dataPort >> 8) & 0xFF, dataPort & 0xFF);
    reply(client, buf);
}

void FtpServer::beginAwaitData(FtpClient& client, FtpClient::PendingKind kind, const std::string& name) {
    if (client.pasvFd < 0) {
        reply(client, "425 Use PASV first\r\n");
        return;
    }
    reply(client, "150 Opening data connection\r\n");
    client.op = FtpClient::Op::AwaitData;
    client.pendingKind = kind;
    client.pendingName = name;
    client.dataWaitDeadlineMs = nowMs() + dataConnectTimeoutMs;
}

bool FtpServer::acceptClientData(FtpClient& client) {
    if (client.pasvFd < 0) {
        return false;
    }
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    int fd = accept(client.pasvFd, reinterpret_cast<sockaddr*>(&addr), &len);
    if (fd < 0) {
        return false;
    }
    setNonBlocking(fd);
    // This connection only ever carries one big sequential transfer, never
    // small interactive writes — Nagle's algorithm (batching small writes
    // to wait for an ACK) has nothing to buy here and can only add latency
    // between chunks, so it's turned off. Standard for a bulk-transfer
    // socket; failure is silently ignored — if the platform doesn't
    // support it, transfers still work, just without this one optimization.
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    close(client.pasvFd);
    client.pasvFd = -1;
    client.dataFd = fd;
    return true;
}

void FtpServer::beginListSend(FtpClient& client) {
    if (atVirtualTop(client)) {
        titles(); // ensures m_titleCache is loaded before buildTitleListing() reads it
        client.sendBuffer = buildTitleListing(client);
        client.sendOffset = 0;
        client.op = FtpClient::Op::ListSend;
        return;
    }
    if (m_root == FtpRoot::InstalledGames && client.virtualTitleId != 0) {
        client.sendBuffer = buildTitleContentListing(client);
        client.sendOffset = 0;
        client.op = FtpClient::Op::ListSend;
        return;
    }

    std::string dirPath = localPath(client, "");
    std::string listing;
    DIR* dir = opendir(dirPath.c_str());
    if (dir) {
        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            if (std::strcmp(ent->d_name, ".") == 0 || std::strcmp(ent->d_name, "..") == 0) {
                continue;
            }
            std::string full = dirPath;
            if (!full.empty() && full.back() != '/') {
                full += "/";
            }
            full += ent->d_name;

            struct stat st{};
            stat(full.c_str(), &st);
            bool isDir = S_ISDIR(st.st_mode);

            char dateBuf[32];
            struct tm* tmv = std::localtime(&st.st_mtime);
            if (tmv) {
                std::strftime(dateBuf, sizeof(dateBuf), "%b %e %Y", tmv);
            } else {
                std::strcpy(dateBuf, "Jan  1 1970");
            }

            char line[600];
            std::snprintf(line, sizeof(line), "%s 1 switch switch %lld %s %s\r\n",
                          isDir ? "drwxr-xr-x" : "-rw-r--r--",
                          isDir ? 0LL : static_cast<long long>(st.st_size),
                          dateBuf, ent->d_name);
            listing += line;
        }
        closedir(dir);
    }

    client.sendBuffer = std::move(listing);
    client.sendOffset = 0;
    client.op = FtpClient::Op::ListSend;
}

void FtpServer::beginRetrSend(FtpClient& client, const std::string& name) {
    // Either inside a title (its own Base/Update file) or at the top level
    // (the merged "[B+U]" file, see buildTitleListing) — both populate
    // client.virtualContentFiles, which beginContentRetr() looks up by name.
    if (m_root == FtpRoot::InstalledGames && (client.virtualTitleId != 0 || atVirtualTop(client))) {
        if (!beginContentRetr(client, name)) {
            closeClientData(client);
            reply(client, "550 Could not read content\r\n");
            client.op = FtpClient::Op::Idle;
        }
        return;
    }

    std::string path = localPath(client, name);
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) {
        closeClientData(client);
        reply(client, "550 File not found\r\n");
        client.op = FtpClient::Op::Idle;
        return;
    }
    fseek(f, 0, SEEK_END);
    long total = ftell(f);
    fseek(f, 0, SEEK_SET);

    client.file = f;
    client.transfer = TransferInfo{};
    client.transfer.isUpload = false;
    client.transfer.name = name;
    client.transfer.directory = reportedCwd(client);
    client.transfer.bytesTotal = total;
    client.transfer.startMs = nowMs();
    client.sendBuffer.clear();
    client.sendOffset = 0;
    client.lastSpeedSampleMs = client.transfer.startMs;
    client.lastSpeedSampleBytes = 0;
    client.op = FtpClient::Op::RetrSend;
}

// Resolves `filename` against the content listing built when this level
// (a title's folder, or the InstalledGames top level for a merged "[B+U]"
// file) was listed — see buildTitleContentListing()/buildTitleListing() —
// to one or more ncm metas, re-gathers their content items the same way
// that sized them for the listing, and sets the client up to stream the
// result: the header goes out first via the normal sendBuffer path (it's
// small, built once up front), then stepTransferOnce()'s RetrSend refill —
// once it sees retrFromContent — pulls each queued content item's bytes in
// turn via ncmContentStorageReadContentIdFile() instead of fread(). The
// content-storage handle itself is opened lazily there, not here, since a
// merged file's items can span two different storages. Returns false
// (nothing opened) if the name doesn't match or any ncm call fails.
// Finds a generated .nsp by the name a client asked for, building the
// lookup table first if this client hasn't listed the directory yet.
//
// The table is normally filled in as a side effect of listing, which is
// enough for an interactive client — Finder, FileZilla and Ferry's own Mac
// app all list before they touch a file. Anything that goes straight to the
// file does not: curl's CWD-then-SIZE-then-RETR walk, a script, a client
// working from a cached listing. Those used to be told a file that is right
// there doesn't exist. Costs one ncm scan, and only on the miss.
const std::vector<VirtualContentEntry>* FtpServer::lookupVirtualContent(FtpClient& client,
                                                                        const std::string& filename) {
    auto find = [&client, &filename]() -> const std::vector<VirtualContentEntry>* {
        for (const auto& entry : client.virtualContentFiles) {
            if (entry.first == filename) {
                return &entry.second;
            }
        }
        return nullptr;
    };

    if (const std::vector<VirtualContentEntry>* hit = find()) {
        return hit;
    }
    if (atVirtualTop(client)) {
        titles(); // as in beginListSend: m_titleCache must be loaded first
        buildTitleListing(client);
    } else if (m_root == FtpRoot::InstalledGames && client.virtualTitleId != 0) {
        buildTitleContentListing(client);
    }
    return find();
}

bool FtpServer::beginContentRetr(FtpClient& client, const std::string& filename) {
    const std::vector<VirtualContentEntry>* metas = lookupVirtualContent(client, filename);
    if (!metas) {
        return false;
    }

    std::vector<NspContentItem> items = gatherItems(*metas);
    if (items.empty()) {
        return false;
    }

    s64 dataSize = 0;
    std::string header = buildPfs0Header(items, dataSize);

    client.retrFromContent = true;
    client.retrContentStorageId = NcmStorageId_None;
    client.retrContentQueue.clear();
    client.retrContentQueue.reserve(items.size());
    for (const auto& item : items) {
        client.retrContentQueue.push_back(RetrContentItem{ item.id, item.size, item.storageId });
    }
    client.retrContentQueueIndex = 0;
    client.retrContentItemOffset = 0;
    client.retrContentReadMs = 0;
    client.retrContentSendMs = 0;

    client.transfer = TransferInfo{};
    client.transfer.isUpload = false;
    client.transfer.name = filename;
    client.transfer.directory = reportedCwd(client);
    client.transfer.bytesTotal = static_cast<long long>(header.size()) + dataSize;
    client.transfer.startMs = nowMs();
    client.sendBuffer = header;
    client.sendOffset = 0;
    client.lastSpeedSampleMs = client.transfer.startMs;
    client.lastSpeedSampleBytes = 0;
    client.op = FtpClient::Op::RetrSend;
    return true;
}

void FtpServer::beginStorRecv(FtpClient& client, const std::string& name) {
    std::string path;
    if (m_root == FtpRoot::InstalledGames && atVirtualTop(client)) {
        // Dropping an install file at the InstalledGames top level — just
        // a plain SD-card write via the same generic path every other STOR
        // uses below, landing somewhere the user can point DBI's own
        // installer at afterward. Deliberately not parsed or installed by
        // Ferry itself — see feedback_ferry_nand_titles_safety memory.
        mkdir("sdmc:/switch", 0777);
        mkdir("sdmc:/switch/Ferry", 0777);
        mkdir("sdmc:/switch/Ferry/Installs", 0777);
        path = "sdmc:/switch/Ferry/Installs/" + name;
    } else {
        path = localPath(client, name);
    }
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        closeClientData(client);
        reply(client, "550 Could not create file\r\n");
        client.op = FtpClient::Op::Idle;
        return;
    }

    // Must happen before any I/O on the stream — see uploadWriteBufferSize.
    client.fileBuffer.resize(uploadWriteBufferSize);
    setvbuf(f, client.fileBuffer.data(), _IOFBF, client.fileBuffer.size());

    client.file = f;
    client.transfer = TransferInfo{};
    client.transfer.isUpload = true;
    client.transfer.name = name;
    client.transfer.directory = reportedCwd(client);
    client.transfer.bytesTotal = client.pendingAlloBytes; // -1 (unknown) if no ALLO preceded this STOR
    client.pendingAlloBytes = -1;
    client.transfer.startMs = nowMs();
    client.recvChunk.resize(transferChunkSize);
    client.lastSpeedSampleMs = client.transfer.startMs;
    client.lastSpeedSampleBytes = 0;
    client.storRecvMs = 0;
    client.storWriteMs = 0;
    client.op = FtpClient::Op::StorRecv;
}

// Advances whichever operation `client.op` names, repeatedly calling
// stepTransferOnce() until it hits EWOULDBLOCK (nothing more available
// right now), finishes, or the budget runs out. A single non-blocking
// send()/recv() only moves whatever happens to already be sitting in the
// kernel socket buffer at that instant, so looping until genuinely
// blocked is what gets real throughput rather than one chunk per pass.
//
// The budget is now only a fairness valve between clients — it used to
// double as the frame's I/O time slice, back when this ran on the main
// thread and every millisecond spent here was a millisecond the renderer
// didn't get. On the server thread there's no frame to protect, so it's
// sized generously: the loop almost always exits on EWOULDBLOCK long
// before this, and the cap only matters when several clients are
// saturating the link at once.
bool FtpServer::stepTransfer(FtpClient& client) {
    constexpr long long budgetMs = 100;
    long long deadline = nowMs() + budgetMs;
    bool moved = false;
    while (nowMs() < deadline) {
        if (!stepTransferOnce(client)) {
            break; // would block, or just finished/failed — nothing more to do now
        }
        moved = true;
        if (client.op != FtpClient::Op::ListSend && client.op != FtpClient::Op::RetrSend &&
            client.op != FtpClient::Op::StorRecv) {
            break; // finished
        }
    }
    return moved;
}

// transfer.speedBps as bytesDone/elapsedSinceStart (a cumulative average)
// spikes wildly right after a transfer starts — a tiny elapsed time in the
// denominator — and afterwards only crawls toward the real rate instead of
// tracking it. Sampled here every ~200ms as bytes-since-last-sample and
// blended into a running average instead, so it reflects the current rate.
static void sampleSpeed(FtpClient& client, long long nowMsValue) {
    long long dt = nowMsValue - client.lastSpeedSampleMs;
    if (dt < 200) {
        return;
    }
    double instant = static_cast<double>(client.transfer.bytesDone - client.lastSpeedSampleBytes) * 1000.0 / dt;
    client.transfer.speedBps = client.transfer.speedBps <= 0 ? instant : (0.35 * instant + 0.65 * client.transfer.speedBps);
    client.lastSpeedSampleMs = nowMsValue;
    client.lastSpeedSampleBytes = client.transfer.bytesDone;
}

// Performs one send()/recv() (or file-refill) step. Returns true if it
// actually moved data and the caller should keep looping, false if the
// socket would block right now (or the operation just ended).
bool FtpServer::stepTransferOnce(FtpClient& client) {
    switch (client.op) {
        case FtpClient::Op::ListSend: {
            if (client.sendOffset >= client.sendBuffer.size()) {
                finishList(client, true);
                return false;
            }
            ssize_t n = send(client.dataFd, client.sendBuffer.data() + client.sendOffset,
                              client.sendBuffer.size() - client.sendOffset, 0);
            if (n > 0) {
                client.sendOffset += static_cast<size_t>(n);
                if (client.sendOffset >= client.sendBuffer.size()) {
                    finishList(client, true);
                }
                return true;
            }
            if (n < 0 && wouldBlock()) {
                return false;
            }
            finishList(client, false);
            return false;
        }
        case FtpClient::Op::RetrSend: {
            if (client.sendOffset >= client.sendBuffer.size()) {
                if (client.retrFromContent) {
                    // The header (sent first, see beginContentRetr) is
                    // already behind us once this branch runs at all — from
                    // here it's each queued content item's raw bytes back to
                    // back. Reads are exact-size, unlike fread — no short-
                    // read/EOF signal — so remaining is tracked per-item
                    // against its own queried size, and the loop advances to
                    // the next item (skipping any that turn out empty)
                    // rather than treating the whole transfer as one range,
                    // since each item's ncm read offset resets to 0.
                    bool gotChunk = false;
                    while (client.retrContentQueueIndex < client.retrContentQueue.size()) {
                        const RetrContentItem& item = client.retrContentQueue[client.retrContentQueueIndex];
                        s64 remaining = item.size - client.retrContentItemOffset;
                        if (remaining <= 0) {
                            client.retrContentQueueIndex++;
                            client.retrContentItemOffset = 0;
                            continue;
                        }
                        // A merged "[B+U]" file's items can span two
                        // different storages (base on one, update on the
                        // other) — reopen only when the item at hand
                        // actually differs from whichever storage the
                        // handle is currently scoped to.
                        if (item.storageId != client.retrContentStorageId) {
                            if (client.retrContentStorageId != NcmStorageId_None) {
                                ncmContentStorageClose(&client.retrContentStorage);
                            }
                            if (R_FAILED(ncmOpenContentStorage(&client.retrContentStorage, item.storageId))) {
                                finishTransfer(client, false);
                                return false;
                            }
                            client.retrContentStorageId = item.storageId;
                        }
                        size_t chunkLen = static_cast<size_t>(
                            std::min<s64>(remaining, static_cast<s64>(contentReadChunkSize)));
                        client.sendBuffer.resize(chunkLen);
                        long long readStartMs = nowMs();
                        Result rc = ncmContentStorageReadContentIdFile(
                            &client.retrContentStorage, &client.sendBuffer[0], chunkLen, &item.id,
                            client.retrContentItemOffset);
                        client.retrContentReadMs += nowMs() - readStartMs;
                        if (R_FAILED(rc)) {
                            finishTransfer(client, false);
                            return false;
                        }
                        client.retrContentItemOffset += static_cast<s64>(chunkLen);
                        client.sendOffset = 0;
                        gotChunk = true;
                        break;
                    }
                    if (!gotChunk) {
                        finishTransfer(client, true);
                        return false;
                    }
                } else {
                    client.sendBuffer.resize(transferChunkSize);
                    size_t n = fread(&client.sendBuffer[0], 1, transferChunkSize, client.file);
                    if (n == 0) {
                        finishTransfer(client, true);
                        return false;
                    }
                    client.sendBuffer.resize(n);
                    client.sendOffset = 0;
                }
            }
            long long sendStartMs = nowMs();
            ssize_t n = send(client.dataFd, client.sendBuffer.data() + client.sendOffset,
                              client.sendBuffer.size() - client.sendOffset, 0);
            if (client.retrFromContent) {
                client.retrContentSendMs += nowMs() - sendStartMs;
            }
            if (n > 0) {
                client.sendOffset += static_cast<size_t>(n);
                client.transfer.bytesDone += n;
                sampleSpeed(client, nowMs());
                return true;
            }
            if (n < 0 && wouldBlock()) {
                return false;
            }
            finishTransfer(client, false);
            return false;
        }
        case FtpClient::Op::StorRecv: {
            long long recvStartMs = nowMs();
            ssize_t n = recv(client.dataFd, client.recvChunk.data(), client.recvChunk.size(), 0);
            client.storRecvMs += nowMs() - recvStartMs;
            if (n > 0) {
                long long writeStartMs = nowMs();
                size_t written = std::fwrite(client.recvChunk.data(), 1, static_cast<size_t>(n), client.file);
                client.storWriteMs += nowMs() - writeStartMs;
                if (written != static_cast<size_t>(n)) {
                    finishTransfer(client, false);
                    return false;
                }
                client.transfer.bytesDone += n;
                sampleSpeed(client, nowMs());
                return true;
            }
            if (n == 0) {
                finishTransfer(client, true); // client closed the data connection = done
                return false;
            }
            if (wouldBlock()) {
                return false;
            }
            finishTransfer(client, false);
            return false;
        }
        default:
            return false;
    }
}

void FtpServer::finishList(FtpClient& client, bool ok) {
    closeClientData(client);
    reply(client, ok ? "226 Transfer complete\r\n" : "426 Transfer failed\r\n");
    client.sendBuffer.clear();
    client.sendOffset = 0;
    client.op = FtpClient::Op::Idle;
}

void FtpServer::finishTransfer(FtpClient& client, bool ok) {
    if (client.file) {
        // Timed as write cost for uploads: with a 1MB stdio buffer this
        // flush can be a meaningful chunk of the total, and attributing it
        // anywhere else would make the numbers below misleading.
        long long closeStartMs = nowMs();
        fclose(client.file);
        client.file = nullptr;
        if (client.transfer.isUpload) {
            client.storWriteMs += nowMs() - closeStartMs;
        }
    }
    // Safe only now that the stream is closed — setvbuf's buffer has to
    // outlive it. Released rather than kept so an idle client isn't
    // sitting on a megabyte between transfers.
    client.fileBuffer.clear();
    client.fileBuffer.shrink_to_fit();
    // Captured before the reset below, so a completed content transfer can
    // log how much of its wall-clock time was actually spent inside
    // ncmContentStorageReadContentIdFile() vs. everything else (mostly
    // send()) — see contentReadChunkSize's comment: this is what turns the
    // next hardware run into real data instead of another guessed chunk
    // size or a threading decision made blind.
    bool wasContentTransfer = client.retrFromContent && ok && !client.transfer.isUpload;
    long long contentReadMs = client.retrContentReadMs;
    long long contentSendMs = client.retrContentSendMs;
    if (client.retrFromContent) {
        // May never have actually been opened — see beginContentRetr(),
        // which no longer opens it eagerly since a merged file's storage
        // isn't known until the first item is actually read.
        if (client.retrContentStorageId != NcmStorageId_None) {
            ncmContentStorageClose(&client.retrContentStorage);
            client.retrContentStorageId = NcmStorageId_None;
        }
        client.retrFromContent = false;
    }
    closeClientData(client);

    if (ok) {
        reply(client, "226 Transfer complete\r\n");
        if (client.transfer.isUpload) {
            commitIfSaveMounted(client);
            logEvent(LogEntry::Kind::Uploaded, client.transfer.name, client.transfer.bytesDone);
            m_stats.bytesUp += client.transfer.bytesDone;
            m_stats.filesUp += 1;
            // Same purpose (and same Kind::Error-for-styling reasoning) as
            // the DL line below: says whether an upload's time went to the
            // socket or to the SD card write.
            long long totalMs = std::max<long long>(1, nowMs() - client.transfer.startMs);
            int recvPct = static_cast<int>(client.storRecvMs * 100 / totalMs);
            int writePct = static_cast<int>(client.storWriteMs * 100 / totalMs);
            char buf[160];
            std::snprintf(buf, sizeof(buf), "UL timing: %lldms total, recv=%lldms(%d%%) write=%lldms(%d%%)",
                          totalMs, client.storRecvMs, recvPct, client.storWriteMs, writePct);
            logEvent(LogEntry::Kind::Error, buf);
        } else {
            logEvent(LogEntry::Kind::Downloaded, client.transfer.name, client.transfer.bytesDone);
            m_stats.bytesDown += client.transfer.bytesDone;
            m_stats.filesDown += 1;
            if (wasContentTransfer) {
                // Kind::Error only for the visual treatment (renders the
                // text verbatim, no "Deleted "/"Created folder " prefix,
                // and stands out in the activity log) — this isn't an
                // error, it's timing data for the speed investigation.
                long long totalMs = std::max<long long>(1, nowMs() - client.transfer.startMs);
                int readPct = static_cast<int>(contentReadMs * 100 / totalMs);
                int sendPct = static_cast<int>(contentSendMs * 100 / totalMs);
                char buf[160];
                std::snprintf(buf, sizeof(buf), "DL timing: %lldms total, ncm=%lldms(%d%%) send=%lldms(%d%%)",
                              totalMs, contentReadMs, readPct, contentSendMs, sendPct);
                logEvent(LogEntry::Kind::Error, buf);
            }
        }
    } else {
        reply(client, "426 Transfer failed\r\n");
        logEvent(LogEntry::Kind::Error, (client.transfer.isUpload ? "Failed receiving " : "Failed sending ") +
                                             client.transfer.name);
    }

    client.transfer = TransferInfo{};
    client.sendBuffer.clear();
    client.sendOffset = 0;
    client.op = FtpClient::Op::Idle;
}

void FtpServer::doMkd(FtpClient& client, const std::string& name) {
    std::string path = localPath(client, name);
    if (mkdir(path.c_str(), 0777) == 0) {
        commitIfSaveMounted(client);
        reply(client, "257 \"" + name + "\" created\r\n");
        logEvent(LogEntry::Kind::FolderCreated, name);
    } else {
        reply(client, "550 Could not create directory\r\n");
    }
}

void FtpServer::doRmd(FtpClient& client, const std::string& name) {
    std::string path = localPath(client, name);
    if (rmdir(path.c_str()) == 0) {
        commitIfSaveMounted(client);
        reply(client, "250 Directory removed\r\n");
        logEvent(LogEntry::Kind::Deleted, name);
    } else {
        std::string why = std::strerror(errno);
        reply(client, "550 Could not remove directory: " + why + "\r\n");
        logEvent(LogEntry::Kind::Error, "Remove folder failed: " + name + " (" + why + ")");
    }
}

void FtpServer::doDele(FtpClient& client, const std::string& name) {
    std::string path = localPath(client, name);
    if (remove(path.c_str()) == 0) {
        commitIfSaveMounted(client);
        reply(client, "250 File deleted\r\n");
        logEvent(LogEntry::Kind::Deleted, name);
    } else {
        // The reason travels back to the client rather than being dropped:
        // a bare "550 Could not delete file" is indistinguishable between
        // a read-only mount, a missing file and a file still held open,
        // and the client surfaces this text to the user verbatim.
        std::string why = std::strerror(errno);
        reply(client, "550 Could not delete file: " + why + "\r\n");
        logEvent(LogEntry::Kind::Error, "Delete failed: " + name + " (" + why + ")");
    }
}

void FtpServer::doRnfr(FtpClient& client, const std::string& name) {
    client.renameFrom = localPath(client, name);
    reply(client, "350 Ready for RNTO\r\n");
}

void FtpServer::doRnto(FtpClient& client, const std::string& name) {
    if (client.renameFrom.empty()) {
        reply(client, "503 RNFR required first\r\n");
        return;
    }
    std::string to = localPath(client, name);
    if (rename(client.renameFrom.c_str(), to.c_str()) == 0) {
        commitIfSaveMounted(client);
        reply(client, "250 Renamed\r\n");
        logEvent(LogEntry::Kind::Renamed, name);
    } else {
        reply(client, "550 Could not rename\r\n");
    }
    client.renameFrom.clear();
}

void FtpServer::doSize(FtpClient& client, const std::string& name) {
    // The generated .nsp files have no path to stat — their size is the
    // PFS0 header plus every content record, the same figure the listing
    // reports. curl asks SIZE before it will start a download at all, so
    // without this it refuses to fetch them.
    if (m_root == FtpRoot::InstalledGames && (client.virtualTitleId != 0 || atVirtualTop(client))) {
        if (const std::vector<VirtualContentEntry>* metas = lookupVirtualContent(client, name)) {
            std::vector<NspContentItem> items = gatherItems(*metas);
            if (!items.empty()) {
                char buf[48];
                std::snprintf(buf, sizeof(buf), "213 %lld\r\n",
                              static_cast<long long>(pfs0TotalSize(items)));
                reply(client, buf);
                return;
            }
        }
    }

    std::string path = localPath(client, name);
    struct stat st{};
    if (stat(path.c_str(), &st) == 0 && !S_ISDIR(st.st_mode)) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "213 %lld\r\n", static_cast<long long>(st.st_size));
        reply(client, buf);
    } else {
        reply(client, "550 Could not get file size\r\n");
    }
}

void FtpServer::closeClient(FtpClient& client) {
    if (client.disconnected) {
        return;
    }
    if (client.file) {
        fclose(client.file);
        client.file = nullptr;
    }
    unmountClientVirtual(client);
    closeClientData(client);
    if (client.controlFd >= 0) {
        close(client.controlFd);
        client.controlFd = -1;
    }
    logEvent(LogEntry::Kind::Disconnected, client.ip);
    client.disconnected = true;
}

void FtpServer::closeClientData(FtpClient& client) {
    if (client.pasvFd >= 0) {
        close(client.pasvFd);
        client.pasvFd = -1;
    }
    if (client.dataFd >= 0) {
        close(client.dataFd);
        client.dataFd = -1;
    }
}

std::string FtpServer::primaryClientIp() const {
    return m_clients.empty() ? std::string() : m_clients.front().ip;
}

std::vector<TransferInfo> FtpServer::activeTransfers() const {
    std::vector<TransferInfo> result;
    for (const auto& client : m_clients) {
        if (client.op == FtpClient::Op::RetrSend || client.op == FtpClient::Op::StorRecv) {
            result.push_back(client.transfer);
        }
    }
    return result;
}

void FtpServer::logEvent(LogEntry::Kind kind, const std::string& text, long long size) {
    long long completedAtMs = static_cast<long long>(std::time(nullptr)) * 1000;
    m_log.push_back(LogEntry{ kind, text, size, completedAtMs });
    while (m_log.size() > 10) {
        m_log.pop_front();
    }
    appendToLogFile(kind, text, size);
}

void FtpServer::appendToLogFile(LogEntry::Kind kind, const std::string& text, long long size) {
    mkdir("sdmc:/switch", 0777);
    mkdir("sdmc:/switch/Ferry", 0777);
    FILE* f = fopen("sdmc:/switch/Ferry/log.txt", "a");
    if (!f) {
        return;
    }

    time_t t = std::time(nullptr);
    struct tm* tmv = std::localtime(&t);
    char stamp[32];
    if (tmv) {
        std::strftime(stamp, sizeof(stamp), "%Y-%m-%d %H:%M:%S", tmv);
    } else {
        std::strcpy(stamp, "----");
    }

    const char* verb = "";
    switch (kind) {
        case LogEntry::Kind::Connected: verb = "Connected"; break;
        case LogEntry::Kind::Disconnected: verb = "Disconnected"; break;
        case LogEntry::Kind::Uploaded: verb = "Received"; break;
        case LogEntry::Kind::Downloaded: verb = "Sent"; break;
        case LogEntry::Kind::Deleted: verb = "Deleted"; break;
        case LogEntry::Kind::FolderCreated: verb = "Created folder"; break;
        case LogEntry::Kind::Renamed: verb = "Renamed to"; break;
        case LogEntry::Kind::Error: verb = "Error"; break;
    }

    std::fprintf(f, "[%s] %s %s", stamp, verb, text.c_str());
    if (size > 0) {
        std::fprintf(f, " (%lld bytes)", size);
    }
    std::fprintf(f, "\n");
    fclose(f);
}
