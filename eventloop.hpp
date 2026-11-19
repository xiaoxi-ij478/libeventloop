#ifndef EVENTLOOP_H_INCLUDED
#define EVENTLOOP_H_INCLUDED

#include <chrono>
#include <queue>
#include <unordered_map>
#include <functional>

class EventLoop
{
        int event_queue_pipe_fd[2];
        unsigned pending_events;
        std::unordered_map<int, std::function<void(int)>> timer_fds, manual_fds;
        std::unordered_map<unsigned, std::function<void(unsigned)>> events;

    public:
        EventLoop();

        bool add_event_listener(
            unsigned eventid,
            const std::function<void(unsigned)> &func
        );
        bool add_fd_listener(int fd, const std::function<void(int)> &func);
        int set_timeout(
            const std::chrono::nanoseconds &timeout,
            const std::function<void(int)> &func
        );
        int set_interval(
            const std::chrono::nanoseconds &interval,
            const std::function<void(int)> &func
        );
        int after_idle(const std::function<void()> &func);
        void trigger_event(unsigned eventid);
        bool remove_event_listener(unsigned eventid);
        bool remove_fd_listener(int fd);
        bool clear_timeout(int handle);
        bool clear_interval(int handle);
        void mainloop();
};


#endif // EVENTLOOP_H_INCLUDED
