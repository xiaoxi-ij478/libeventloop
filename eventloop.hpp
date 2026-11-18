#ifndef EVENTLOOP_H_INCLUDED
#define EVENTLOOP_H_INCLUDED

#include <chrono>
#include <queue>
#include <unordered_map>
#include <functional>

struct TimerInfo {
    std::chrono::steady_clock::time_point last_time;
    std::chrono::milliseconds interval;
    bool oneshot;
    std::function<void(unsigned)> callback;
};

class EventLoop
{
        unsigned event_loop_interval_us;
        unsigned last_timer_id;
        unsigned last_event_id;
        std::queue<unsigned> event_queue;
        std::unordered_map<unsigned, TimerInfo> timers;
        std::unordered_map<int, std::function<void(int)>> manual_fds;
        // *INDENT-OFF*
        std::unordered_map<
            unsigned, // event id
            std::unordered_map<
                unsigned, // event handle
                std::function<void(unsigned, unsigned)> // callback(event id, event handle)
            >
        > events;
        // *INDENT-ON*

    public:
        EventLoop(unsigned event_loop_interval_us = 1000);

        unsigned add_event_listener(
            unsigned eventid,
            const std::function<void(unsigned, unsigned)> &func
        );
        void add_fd_listener(int fd, const std::function<void(int)> &func);
        void remove_event_listener(unsigned eventid, unsigned handle);
        void remove_fd_listener(int fd);
        void trigger_event(unsigned eventid);
        unsigned set_timeout(
            const std::chrono::milliseconds &timeout,
            const std::function<void(unsigned)> &func
        );
        unsigned set_interval(
            const std::chrono::milliseconds &interval,
            const std::function<void(unsigned)> &func
        );
        void clear_timeout(unsigned handle);
        void clear_interval(unsigned handle);
        void mainloop();
};


#endif // EVENTLOOP_H_INCLUDED
