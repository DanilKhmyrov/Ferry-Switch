#pragma once

#include <cstdint>
#include <cstdio>
#include <deque>
#include <string>
#include <vector>

#include "titles.hpp"

struct TransferInfo {
    bool isUpload = false;     // true = STOR (receiving), false = RETR (sending)
    std::string name;
    std::string directory;    // remote path it's landing in/coming from, e.g. "/switch/"
    long long bytesDone = 0;
    long long bytesTotal = -1; // -1 = unknown (STOR doesn't declare a size upfront)
    long long startMs = 0;
    double speedBps = 0;      // smoothed current rate — see stepTransferOnce()
};

struct LogEntry {
    enum class Kind {
        Connected,
        Disconnected,
        Uploaded,
        Downloaded,
        Deleted,
        FolderCreated,
        Renamed,
        Error,
    };
    Kind kind;
    std::string text;
    long long size = 0;
    long long completedAtMs = 0; // wall-clock ms (CLOCK_REALTIME), for display only
};

struct SessionStats {
    long long bytesUp = 0;   // received (STOR), summed across every client
    long long bytesDown = 0; // sent (RETR), summed across every client
    int filesUp = 0;
    int filesDown = 0;
};

// Which filesystem the server exposes:
//  - Sd: sdmc:/ — the original behavior, read-write.
//  - Nand: the console's internal User BIS partition, mounted read-only.
//  - InstalledGames/Saves: synthetic — the top level is a generated list
//    of installed titles (see titles.hpp), not a real directory; entering
//    one resolves to that title's actual content/save data.
// Everything except Sd is strictly read-only, enforced centrally in
// handleCommand() rather than scattered across individual command
// handlers, since a mistake here risks the console's own data rather than
// just a bug in the app.
enum class FtpRoot { Sd, Nand, InstalledGames, Saves };

// One ncm meta backing part of a virtual .nsp download — which meta and
// which storage location it lives on. Installed titles can live on either
// the console's internal storage or the SD card, and an
// NcmContentMetaDatabase/NcmContentStorage handle is scoped to exactly
// one of those, so both the listing pass and the later RETR have to know
// which one a given entry came from rather than assuming. A single .nsp
// download packages *one or more* of these — one for a title's own Base
// or Update file, two (base+update) for the merged top-level "[B+U]"
// convenience file DBI also offers — see FtpClient::virtualContentFiles.
struct VirtualContentEntry {
    NcmContentMetaKey key;
    NcmStorageId storageId;
};

// One item in FtpClient::retrContentQueue — a content id/size pair plus
// which storage it has to be read from. The merged "[B+U]" file can draw
// items from two different metas that don't necessarily live on the same
// storage (base on one, update on the other), so unlike the single shared
// NcmContentStorage handle this used to assume, the storage has to travel
// with each item — see stepTransferOnce()'s RetrSend refill.
struct RetrContentItem {
    NcmContentId id;
    s64 size;
    NcmStorageId storageId;
};

// One connected client's session state — a plain, cheaply-movable struct
// (not a class) so FtpServer can just hold `std::vector<FtpClient>` and let
// the vector manage lifetime, matching the rest of this codebase's
// preference for manual resource handling over RAII wrappers.
struct FtpClient {
    int controlFd = -1;
    int pasvFd = -1; // listening, between PASV and the client opening its data connection
    int dataFd = -1; // the accepted data connection, while a transfer/listing is in flight

    std::string ip;
    std::string recvBuffer;
    std::string cwd = "/";
    bool awaitingPassword = false;
    std::string renameFrom;
    bool disconnected = false; // set by closeClient(); FtpServer::poll() sweeps these out

    // Set by ALLO (if the client sends one) and consumed by the next
    // beginStorRecv() — an upload's size is otherwise unknown to this
    // server until the transfer finishes, so the live progress display
    // has nothing to show a percentage/total against. -1 = none pending.
    // The Ferry Mac client sends this (it always knows a local file's
    // size upfront); a generic FTP client that doesn't bother is still
    // fine, just back to the old "bytes so far" display for that upload.
    long long pendingAlloBytes = -1;

    // Purely cosmetic prefix — what PWD (and transfer-log entries) report
    // to the client — for the title name once inside a synthetic
    // InstalledGames/Saves folder. Kept separate from `cwd` itself rather
    // than folding the title name into it, since for Saves `cwd` doubles
    // as the *real* path relative to the mounted save volume (consumed by
    // localPath()/changeDirectory()) and that volume has no such folder at
    // its root — prefixing it there would silently break every real file
    // operation once inside a save. Empty = report cwd as-is. Set in
    // enterTitle(), cleared in leaveTitle(). See reportedCwd().
    std::string virtualCwdDisplay;

    // Stable for the connection's lifetime (unlike its index in
    // FtpServer::m_clients, which shifts as other clients disconnect) —
    // used to build a unique per-client save-data mount name, since two
    // clients could each be browsing a different title's save at once.
    int slotId = -1;

    // Set while this client has entered a title's save data under the
    // Saves root — `virtualMountName + ":"` is what localPath() uses as
    // the prefix instead of the server-wide m_rootPrefix, and it's
    // unmounted again on CDUP back to the title list or on disconnect.
    bool virtualMounted = false;
    std::string virtualMountName;

    // Set while this client has navigated into the "Installs" folder
    // under the InstalledGames root — a real SD folder (Ferry's own
    // install-staging area, sdmc:/switch/Ferry/Installs), not a synthetic
    // one, so unlike everything else under InstalledGames it's fully
    // read-write: the user's own uploads, not installed-title data.
    // localPath() swaps in that folder's absolute path as the prefix
    // while this is set, the same way virtualMountName does for a mounted
    // save. Cleared on CDUP back to the top level.
    bool installsEntered = false;

    // Set while this client is inside a title's synthetic content listing
    // under the InstalledGames root (0 = at the top-level title list).
    // filename -> the one or more ncm metas that filename's download
    // should package together into one .nsp — a single entry for a
    // title's own Base/Update file (see buildTitleContentListing), two
    // (base+update) for the merged top-level "[B+U]" convenience file
    // (see buildTitleListing). Rebuilt each time the corresponding listing
    // is generated, consulted when RETR asks for one of those filenames —
    // used at *either* level, never both at once, since a client is always
    // either at the top of InstalledGames or inside exactly one title.
    u64 virtualTitleId = 0;
    std::vector<std::pair<std::string, std::vector<VirtualContentEntry>>> virtualContentFiles;

    // Set for the duration of a RETR that's streaming a dynamically-built
    // virtual .nsp (a real PFS0 container assembled from installed-title
    // content via ncm, not a real file) instead of reading from disk —
    // stepTransferOnce() branches on this. The PFS0 header/string table is
    // sent first via the normal sendBuffer path (it's built once, up
    // front, and is small); once that's drained, refill instead pulls
    // from this queue — each content item's raw bytes back to back — via
    // ncmContentStorageReadContentIdFile(), advancing to the next item
    // when the current one is exhausted. retrContentStorage is reopened
    // against a queue item's own storage whenever it differs from
    // retrContentStorageId (the storage the currently-open handle is
    // scoped to — NcmStorageId_None means nothing has been opened yet).
    // Closed in finishTransfer()/unmountClientVirtual(), guarded on
    // retrContentStorageId since it may never have been opened at all for
    // a transfer that failed before its first byte.
    bool retrFromContent = false;
    NcmContentStorage retrContentStorage{};
    NcmStorageId retrContentStorageId = NcmStorageId_None;
    std::vector<RetrContentItem> retrContentQueue;
    size_t retrContentQueueIndex = 0;
    s64 retrContentItemOffset = 0;
    // Cumulative time spent inside ncmContentStorageReadContentIdFile()
    // itself for this transfer, separate from time spent in send() —
    // logged at finishTransfer() so a real hardware run says whether the
    // ncm read or the network send is actually where the time goes,
    // instead of guessing at chunk sizes again. Reset in beginContentRetr().
    // retrContentSendMs is the same idea for the send() side — between the
    // two, whatever's left of the transfer's total wall time is neither
    // reading nor sending, i.e. loop/tick-rate overhead.
    long long retrContentReadMs = 0;
    long long retrContentSendMs = 0;

    enum class Op {
        Idle,
        AwaitData, // replied 150, waiting for the client to open the data connection
        ListSend,
        RetrSend,
        StorRecv,
    };
    Op op = Op::Idle;

    enum class PendingKind { None, List, Retr, Stor };
    PendingKind pendingKind = PendingKind::None;
    std::string pendingName;
    long long dataWaitDeadlineMs = 0;

    FILE* file = nullptr;
    // Backing store for the upload FILE*'s stdio buffer (installed via
    // setvbuf in beginStorRecv). Owned here rather than left to stdio
    // because setvbuf does not copy it — it has to stay alive for as long
    // as the stream does. See uploadWriteBufferSize.
    std::vector<char> fileBuffer;
    std::string sendBuffer;   // ListSend (whole listing) / RetrSend (one chunk at a time)
    size_t sendOffset = 0;
    std::vector<char> recvChunk; // StorRecv's reusable recv() target, sized once per transfer
    TransferInfo transfer;       // valid while op is RetrSend/StorRecv

    // Rolling window for transfer.speedBps — see stepTransferOnce().
    long long lastSpeedSampleMs = 0;
    long long lastSpeedSampleBytes = 0;

    // Upload-side equivalents of retrContentReadMs/retrContentSendMs:
    // how much of an upload's wall time went to pulling bytes off the
    // socket vs. writing them to the SD card. Logged at finishTransfer()
    // so a hardware run says which half is actually the bottleneck
    // instead of it being guessed at.
    long long storRecvMs = 0;
    long long storWriteMs = 0;
};

// Everything the UI draws, copied out in one consistent piece. The server
// runs on its own thread (see FtpServer), so the UI can't hold references
// into live server state while it renders — it takes one of these per
// frame instead, under a lock held only for the copy itself.
struct ServerSnapshot {
    std::string localIp;
    size_t clientCount = 0;
    std::string primaryClientIp;
    std::vector<TransferInfo> transfers;
    std::deque<LogEntry> log;
    SessionStats stats;
    bool pinProtected = false;
    FtpRoot root = FtpRoot::Sd;
};

// A non-blocking, cooperatively-multiplexed FTP server tailored to what the
// Ferry macOS client actually sends: USER/PASS/TYPE/PWD/CWD/CDUP/PASV/LIST/
// RETR/STOR/MKD/RMD/DELE/RNFR/RNTO/SIZE/QUIT. Several clients can be
// connected — and mid-transfer — at the same time, which is what makes the
// Mac app's parallel-transfer mode actually run in parallel against a
// Switch target. Every socket is non-blocking and each client's current
// command or transfer is advanced by one bounded step per poll().
//
// **Threading.** poll() runs on a dedicated thread, not the frame loop.
// It used to share the main thread, which capped throughput hard: the
// frame loop is vsync-paced, so transfers only got ~60 chances a second
// to move data and spent the rest of each frame parked in
// SDL_RenderPresent — measured at ~77% of an upload's wall time going to
// neither the socket nor the SD card, just waiting for the next tick.
//
// The concurrency rule that keeps this safe is deliberately narrow: the
// server thread **exclusively owns** every member below except the ones
// grouped under m_lock. It never takes the lock while doing socket or
// file I/O — only to publish a snapshot, or to pick up requests from the
// UI. The UI thread never touches server state directly: it reads
// snapshot() and asks for changes through requestRoot()/setPin()/
// requestDisconnectAll(), which the server thread applies between polls.
// That's what makes root switching and NAND mounting safe without
// locking the whole server, since those then only ever happen on the one
// thread that reads them.
class FtpServer {
public:
    bool start(uint16_t port);
    void stop();

    uint16_t port() const { return m_port; }

    /// One consistent copy of everything the UI renders. Cheap — a few
    /// small containers — and takes the lock only for the copy.
    ServerSnapshot snapshot() const;

    /// Asks the server thread to switch roots; applied before its next
    /// poll(). Silently does nothing if a client is connected by then
    /// (switching the filesystem out from under an active session would
    /// leave its `cwd` pointing at nothing) or if mounting NAND fails —
    /// the UI reflects whatever actually took effect via snapshot().root
    /// on a later frame rather than assuming this succeeded.
    void requestRoot(FtpRoot root);

    /// Forcibly drops every connected client — for the Switch-side "kick"
    /// control, when a connection needs resetting without touching the Mac
    /// client (a hung transfer, or just wanting to switch roots without
    /// waiting for the other side to disconnect on its own). Applied on
    /// the server thread, before the root request above, so asking for
    /// both at once works.
    void requestDisconnectAll();

    // Empty = anonymous, no password required (the original behavior).
    // When set, USER replies 331 (password required) instead of 230, and
    // PASS must match exactly or the client is kicked with 530 — checked
    // against what the FTP client sends as its password, so on the Ferry
    // Mac side this is just the "password" field on the connection.
    // Applied on the server thread like the requests above, so m_pin stays
    // owned by that one thread and PASS can read it without locking.
    void setPin(const std::string& pin);

private:
    static void threadEntry(void* self);
    void threadMain();
    /// Picks up whatever the UI asked for since the last poll — see the
    /// threading note above. Takes the lock only to drain the requests.
    void applyPendingRequests();
    /// Copies current state into m_snapshot under the lock. Throttled:
    /// the loop runs far faster than the screen refreshes, and there's no
    /// point copying containers thousands of times a second.
    void publishSnapshot();
    /// One pass over every client. Returns true if any of them actually
    /// moved data, which is what decides whether the thread sleeps before
    /// coming back around (so an idle server doesn't spin a core).
    bool poll();
    bool setRoot(FtpRoot root);
    void disconnectAll();
    std::string primaryClientIp() const;
    std::vector<TransferInfo> activeTransfers() const;

    void refreshLocalIp();
    void acceptNewClients();
    bool serviceClient(FtpClient& client);
    void processBuffered(FtpClient& client);
    void handleCommand(FtpClient& client, const std::string& line);
    void reply(FtpClient& client, const std::string& text);
    // What PWD (and transfer-log entries) should show this client as its
    // current path — client.cwd with virtualCwdDisplay prefixed in front
    // when set, otherwise client.cwd verbatim. See FtpClient::virtualCwdDisplay.
    std::string reportedCwd(const FtpClient& client) const;
    void changeDirectory(FtpClient& client, const std::string& arg);
    void enterPassive(FtpClient& client);
    void beginAwaitData(FtpClient& client, FtpClient::PendingKind kind, const std::string& name);
    bool acceptClientData(FtpClient& client);
    void beginListSend(FtpClient& client);
    void beginRetrSend(FtpClient& client, const std::string& name);
    void beginStorRecv(FtpClient& client, const std::string& name);
    bool stepTransfer(FtpClient& client);
    bool stepTransferOnce(FtpClient& client);
    void finishList(FtpClient& client, bool ok);
    void finishTransfer(FtpClient& client, bool ok);
    void doMkd(FtpClient& client, const std::string& name);
    void doRmd(FtpClient& client, const std::string& name);
    void doDele(FtpClient& client, const std::string& name);
    void doRnfr(FtpClient& client, const std::string& name);
    void doRnto(FtpClient& client, const std::string& name);
    void doSize(FtpClient& client, const std::string& name);
    void closeClient(FtpClient& client);
    void closeClientData(FtpClient& client);
    void unmountClientVirtual(FtpClient& client);
    void logEvent(LogEntry::Kind kind, const std::string& text, long long size = 0);
    void appendToLogFile(LogEntry::Kind kind, const std::string& text, long long size);
    std::string rootPrefixFor(const FtpClient& client) const;
    std::string localPath(const FtpClient& client, const std::string& name) const;

    // False only for Sd, and for Saves once this specific client has
    // mounted an actual save (a real, writable filesystem from that point
    // on) — every other case (Nand, InstalledGames at any level, Saves
    // before a title is entered) stays read-only. Per-client rather than
    // global since which client (if any) is "inside" a mounted save is
    // itself per-client state.
    bool isReadOnly(const FtpClient& client) const;
    // STOR gets one extra carve-out beyond isReadOnly(): dropping an
    // install file (.nsp/.nsz/.xci/.xcz) at the InstalledGames top level —
    // see beginStorRecv(). Nothing else (MKD/RMD/DELE/RNFR/RNTO) gets this
    // exception, since none of those have a sensible meaning against the
    // synthetic title listing.
    bool canStor(const FtpClient& client) const;
    // libnx save-data mounts buffer writes until explicitly committed —
    // called after every successful write while a save is mounted
    // (STOR/DELE/RNTO/MKD/RMD), a no-op otherwise.
    void commitIfSaveMounted(FtpClient& client);

    // Virtual-root helpers (InstalledGames/Saves) — see ftp_server.cpp.
    const std::vector<TitleInfo>& titles();
    bool atVirtualTop(const FtpClient& client) const;
    std::string buildTitleListing(FtpClient& client) const;
    std::string buildTitleContentListing(FtpClient& client) const;
    bool enterTitle(FtpClient& client, const std::string& folderName);
    void leaveTitle(FtpClient& client);
    const std::vector<VirtualContentEntry>* lookupVirtualContent(FtpClient& client, const std::string& filename);
    bool beginContentRetr(FtpClient& client, const std::string& filename);

    int m_listenFd = -1;
    uint16_t m_port = 0;

    std::string m_localIp;
    long long m_lastIpRefreshMs = 0;
    std::string m_pin;
    std::deque<LogEntry> m_log;
    SessionStats m_stats;

    std::vector<FtpClient> m_clients;
    int m_nextClientId = 0;

    FtpRoot m_root = FtpRoot::Sd;
    std::string m_rootPrefix = "sdmc:";
    bool m_nandMounted = false;

    std::vector<TitleInfo> m_titleCache;
    bool m_titleCacheLoaded = false;

    // --- server thread ---
    Thread m_thread{};
    bool m_threadStarted = false;
    // Written by stop() on the UI thread, read by the server thread. A
    // plain bool rather than an atomic: it's a one-way latch that only
    // ever goes false→true, and the thread is joined right after, so the
    // worst case is noticing it one iteration late.
    volatile bool m_stopRequested = false;
    long long m_lastSnapshotMs = 0;

    // --- everything below is guarded by m_lock ---
    mutable Mutex m_lock{};
    ServerSnapshot m_snapshot;
    bool m_rootRequested = false;
    FtpRoot m_requestedRoot = FtpRoot::Sd;
    bool m_disconnectRequested = false;
    bool m_pinRequested = false;
    std::string m_requestedPin;
};
