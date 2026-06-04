#pragma once

// Per-peer disconnect-reason counters. closePeer() records why a connection dropped.
// Lock-free; published via HTTP for stuck-tick diagnosis.

#include <atomic>

namespace PeerDisc
{
enum Reason : unsigned int
{
    OTHER = 0,
    RECV_ERR,        // receiveToken status != 0
    RECV_FIN_POLL,   // GetModeData -> Tcp4StateClosed (poll POLLHUP/ERR)
    RECV_INIT_FAIL,  // Receive() returned error / FIN
    XMIT_ERR,        // transmitToken status != 0 (incl shim 1s send timeout)
    XMIT_GETMODE,    // transmitData GetModeData closed
    XMIT_INIT_FAIL,  // Transmit() returned error
    CONNECT_REJECT,  // outgoing connect rejected
    PROTO_VIOLATION, // bad header size -> forget peer
    REASON_COUNT
};

static const char* const kName[REASON_COUNT] = {
    "other", "recvErr", "recvFinPoll", "recvInitFail",
    "xmitErr", "xmitGetmode", "xmitInitFail", "connectReject", "protoViolation"
};

static constexpr unsigned int MAX_SLOTS = 1024;

static std::atomic<unsigned long long> gReasonCount[REASON_COUNT];
static std::atomic<unsigned long long> gTotal{0};
static std::atomic<unsigned int> gLastReason{0};
static std::atomic<unsigned int> gSlotCount[MAX_SLOTS];      // disconnects per peer slot
static std::atomic<unsigned int> gSlotLastReason[MAX_SLOTS];

// Records one real disconnect. Call once per actual close (inside the !isClosing guard).
static inline void note(unsigned int reason, unsigned int slot)
{
    if (reason >= REASON_COUNT) reason = OTHER;
    gReasonCount[reason].fetch_add(1, std::memory_order_relaxed);
    gTotal.fetch_add(1, std::memory_order_relaxed);
    gLastReason.store(reason, std::memory_order_relaxed);
    if (slot < MAX_SLOTS)
    {
        gSlotCount[slot].fetch_add(1, std::memory_order_relaxed);
        gSlotLastReason[slot].store(reason, std::memory_order_relaxed);
    }
}
}
