#include <iostream>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <vector>
#include <string>
#include <iomanip>
#include <cstdint>
#include <cstring>
#include <immintrin.h>

// ============================================================================
// 64-Byte Object Specification
// High-Frequency Trading systems optimize for cache line alignment (64 bytes)
// ============================================================================
struct alignas(64) DataPacket64 {
    uint64_t sequence_number; // 8 bytes
    uint64_t timestamp_ns;   // 8 bytes
    uint8_t  payload[48];     // 48 bytes (total = 64 bytes)

    DataPacket64() : sequence_number(0), timestamp_ns(0) {
        std::memset(payload, 0, sizeof(payload));
    }

    explicit DataPacket64(uint64_t seq) : sequence_number(seq), timestamp_ns(0) {
        std::memset(payload, 0xAB, sizeof(payload));
    }
};

static_assert(sizeof(DataPacket64) == 64, "DataPacket64 size must be exactly 64 bytes!");

// ============================================================================
// Optimized SpinLock Implementation (TATAS - Test and Test-and-Set)
// Uses read-spin on cache line before atomic exchange with CPU pause
// to minimize cache bus invalidation traffic (MESI protocol optimization).
// ============================================================================
class SpinLock {
private:
    alignas(64) std::atomic_flag flag = ATOMIC_FLAG_INIT;

public:
    void lock() noexcept {
        while (flag.test_and_set(std::memory_order_acquire)) {
#if defined(_MSC_VER) || defined(__x86_64__) || defined(_M_X64)
            _mm_pause(); // Yield processor execution pipeline
#endif
        }
    }

    void unlock() noexcept {
        flag.clear(std::memory_order_release);
    }
};

// ============================================================================
// SPSC Queue with Locks (Generic template supporting any LockType)
// Implements a power-of-two circular ring buffer protected by lock.
// ============================================================================
template <typename T, typename LockType>
class SPSCQueueWithLock {
public:
    explicit SPSCQueueWithLock(size_t capacity = 65536) 
        : mCapacity(capacity), mMask(capacity - 1), mHead(0), mTail(0) {
        // Enforce power-of-two capacity for fast bitwise modulo
        if ((capacity & (capacity - 1)) != 0) {
            size_t pow2 = 1;
            while (pow2 < capacity) pow2 <<= 1;
            mCapacity = pow2;
            mMask = mCapacity - 1;
        }
        mBuffer = static_cast<T*>(::operator new(sizeof(T) * mCapacity));
    }

    ~SPSCQueueWithLock() {
        // Drain and destroy remaining active elements
        T item;
        while (try_pop(item)) {}
        ::operator delete(mBuffer);
    }

    SPSCQueueWithLock(const SPSCQueueWithLock&) = delete;
    SPSCQueueWithLock& operator=(const SPSCQueueWithLock&) = delete;
    SPSCQueueWithLock(SPSCQueueWithLock&&) = delete;
    SPSCQueueWithLock& operator=(SPSCQueueWithLock&&) = delete;

    // Non-blocking try_push
    bool try_push(const T& val) {
        std::lock_guard<LockType> lock(mLock);
        if (mHead - mTail >= mCapacity) {
            return false; // Queue full
        }
        new (&mBuffer[mHead & mMask]) T(val);
        ++mHead;
        return true;
    }

    // Blocking / spinning push until space is available or stop requested
    bool push(const T& val, const std::atomic<bool>& stop_flag) {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            if (try_push(val)) {
                return true;
            }
#if defined(_MSC_VER) || defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#endif
        }
        return false;
    }

    // Non-blocking try_pop
    bool try_pop(T& val) {
        std::lock_guard<LockType> lock(mLock);
        if (mTail == mHead) {
            return false; // Queue empty
        }
        val = mBuffer[mTail & mMask];
        mBuffer[mTail & mMask].~T();
        ++mTail;
        return true;
    }

    // Blocking / spinning pop until item available or stop requested
    bool pop(T& val, const std::atomic<bool>& stop_flag) {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            if (try_pop(val)) {
                return true;
            }
#if defined(_MSC_VER) || defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#endif
        }
        // Drain any remaining items after stop flag is set
        return try_pop(val);
    }

    size_t size() const {
        return mHead - mTail;
    }

    bool empty() const {
        return mHead == mTail;
    }

private:
    alignas(64) LockType mLock;
    alignas(64) size_t mHead;
    alignas(64) size_t mTail;
    size_t mCapacity;
    size_t mMask;
    T* mBuffer;
};

// ============================================================================
// Benchmark Runner
// ============================================================================
struct BenchmarkResult {
    std::string lock_name;
    uint64_t total_pushed;
    uint64_t total_popped;
    double duration_seconds;
    double push_ops_per_sec;
    double pop_ops_per_sec;
    double throughput_MB_per_sec;
    double avg_latency_ns;
};

template <typename LockType>
BenchmarkResult run_benchmark(const std::string& lock_name, int run_duration_seconds = 1) {
    SPSCQueueWithLock<DataPacket64, LockType> queue(65536);

    std::atomic<bool> start_signal{false};
    std::atomic<bool> stop_signal{false};
    std::atomic<uint64_t> pushed_count{0};
    std::atomic<uint64_t> popped_count{0};

    // Actual Dedicated Producer Thread
    std::thread producer_thread([&]() {
        // Spin-wait until synchronized benchmark start signal
        while (!start_signal.load(std::memory_order_acquire)) {
            _mm_pause();
        }

        uint64_t seq = 0;
        while (!stop_signal.load(std::memory_order_relaxed)) {
            DataPacket64 pkt(++seq);
            if (queue.try_push(pkt)) {
                pushed_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                _mm_pause();
            }
        }
    });

    // Actual Dedicated Consumer Thread
    std::thread consumer_thread([&]() {
        // Spin-wait until synchronized benchmark start signal
        while (!start_signal.load(std::memory_order_acquire)) {
            _mm_pause();
        }

        DataPacket64 pkt;
        while (!stop_signal.load(std::memory_order_relaxed)) {
            if (queue.try_pop(pkt)) {
                popped_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                _mm_pause();
            }
        }

        // Drain any residual packets left in buffer
        while (queue.try_pop(pkt)) {
            popped_count.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Short stabilization before measuring
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto start_time = std::chrono::high_resolution_clock::now();
    start_signal.store(true, std::memory_order_release);

    // Active benchmark execution period
    std::this_thread::sleep_for(std::chrono::seconds(run_duration_seconds));
    stop_signal.store(true, std::memory_order_release);

    producer_thread.join();
    consumer_thread.join();
    auto end_time = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = end_time - start_time;
    double duration_sec = elapsed.count();

    uint64_t total_push = pushed_count.load();
    uint64_t total_pop = popped_count.load();

    BenchmarkResult res;
    res.lock_name = lock_name;
    res.total_pushed = total_push;
    res.total_popped = total_pop;
    res.duration_seconds = duration_sec;
    res.push_ops_per_sec = total_push / duration_sec;
    res.pop_ops_per_sec = total_pop / duration_sec;
    res.throughput_MB_per_sec = (total_pop * 64.0) / (duration_sec * 1024.0 * 1024.0);
    res.avg_latency_ns = (duration_sec * 1e9) / (total_pop > 0 ? total_pop : 1);

    return res;
}

void print_result(const BenchmarkResult& res) {
    std::cout << "\n=======================================================\n";
    std::cout << " Benchmark Result: " << res.lock_name << "\n";
    std::cout << "=======================================================\n";
    std::cout << " Object Size:           64 bytes\n";
    std::cout << " Duration:              " << std::fixed << std::setprecision(3) << res.duration_seconds << " seconds\n";
    std::cout << " Total Objects Pushed:  " << res.total_pushed << "\n";
    std::cout << " Total Objects Popped:  " << res.total_popped << "\n";
    std::cout << " Push Operations/sec:   " << std::fixed << std::setprecision(2) << res.push_ops_per_sec << " ops/sec ("
              << (res.push_ops_per_sec / 1e6) << " Mops/s)\n";
    std::cout << " Pop Operations/sec:    " << std::fixed << std::setprecision(2) << res.pop_ops_per_sec << " ops/sec ("
              << (res.pop_ops_per_sec / 1e6) << " Mops/s)\n";
    std::cout << " Consumer Throughput:   " << std::fixed << std::setprecision(2) << res.throughput_MB_per_sec << " MB/s\n";
    std::cout << " Avg Latency / Op:      " << std::fixed << std::setprecision(2) << res.avg_latency_ns << " ns\n";
    std::cout << "=======================================================\n";
}

int main() {
    std::cout << "=======================================================\n";
    std::cout << " Assignment 1: SPSC Queue with Locks Benchmark\n";
    std::cout << " Payload: 64-Byte Objects (Cache-Line Aligned)\n";
    std::cout << " Concurrency: 1 Dedicated Producer + 1 Dedicated Consumer\n";
    std::cout << "=======================================================\n";

    // 1. Benchmark with Custom SpinLock
    std::cout << "\n[1/2] Running Benchmark with SpinLock for 1 second..." << std::endl;
    BenchmarkResult spinlock_res = run_benchmark<SpinLock>("SpinLock (CPU Pause)", 1);
    print_result(spinlock_res);

    // 2. Benchmark with std::mutex
    std::cout << "\n[2/2] Running Benchmark with std::mutex for 1 second..." << std::endl;
    BenchmarkResult mutex_res = run_benchmark<std::mutex>("std::mutex", 1);
    print_result(mutex_res);

    // Summary Comparison Table
    std::cout << "\n========================================================================================\n";
    std::cout << " PERFORMANCE COMPARISON (64-byte objects per second)\n";
    std::cout << "========================================================================================\n";
    std::cout << std::left << std::setw(28) << "Lock Mechanism"
              << std::setw(20) << "Push Ops/sec"
              << std::setw(20) << "Pop Ops/sec"
              << std::setw(15) << "Throughput"
              << std::setw(15) << "Avg Latency" << "\n";
    std::cout << "----------------------------------------------------------------------------------------\n";
    std::cout << std::left << std::setw(28) << spinlock_res.lock_name
              << std::setw(20) << (std::to_string(static_cast<uint64_t>(spinlock_res.push_ops_per_sec)) + " ops/s")
              << std::setw(20) << (std::to_string(static_cast<uint64_t>(spinlock_res.pop_ops_per_sec)) + " ops/s")
              << std::setw(15) << (std::to_string(static_cast<uint64_t>(spinlock_res.throughput_MB_per_sec)) + " MB/s")
              << std::setw(15) << (std::to_string(static_cast<uint64_t>(spinlock_res.avg_latency_ns)) + " ns") << "\n";
    std::cout << std::left << std::setw(28) << mutex_res.lock_name
              << std::setw(20) << (std::to_string(static_cast<uint64_t>(mutex_res.push_ops_per_sec)) + " ops/s")
              << std::setw(20) << (std::to_string(static_cast<uint64_t>(mutex_res.pop_ops_per_sec)) + " ops/s")
              << std::setw(15) << (std::to_string(static_cast<uint64_t>(mutex_res.throughput_MB_per_sec)) + " MB/s")
              << std::setw(15) << (std::to_string(static_cast<uint64_t>(mutex_res.avg_latency_ns)) + " ns") << "\n";
    std::cout << "========================================================================================\n";

    return 0;
}
