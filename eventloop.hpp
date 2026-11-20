#ifndef EVENTLOOP_H_INCLUDED
#define EVENTLOOP_H_INCLUDED

#include <chrono>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <memory>

struct EventData {
    EventData() = default;
    virtual ~EventData() = default;
};

class EventLoop
{
        unsigned last_event_handle;
        int event_queue_pipe_fd[2];
        std::unordered_set<int> oneshot_timer_fds;
        // arg1: fd
        std::unordered_map<int, std::function<void(int)>> timer_fds, manual_fds;
        // arg1: event id, arg2: event handle, arg3: data
        std::unordered_map<unsigned, std::unordered_map<unsigned, std::function<void(unsigned, unsigned, std::shared_ptr<EventData>) >>> events;
        std::queue<std::shared_ptr<EventData>> events_data;

    public:
        EventLoop();

        unsigned add_event_listener(
            unsigned eventid,
            const std::function<void(unsigned, unsigned, std::shared_ptr<EventData>) > &func
        );
        void add_fd_listener(int fd, const std::function<void(int)> &func);
        int set_timeout(
            const std::chrono::nanoseconds &timeout,
            const std::function<void(int)> &func
        );
        int set_interval(
            const std::chrono::nanoseconds &interval,
            const std::function<void(int)> &func
        );
        int after_idle(const std::function<void()> &func);
        void trigger_event(unsigned eventid, const std::shared_ptr<EventData> &data);
        void remove_event_listener(unsigned handle);
        void remove_fd_listener(int fd);
        void clear_timeout(int handle);
        void clear_interval(int handle);
        void mainloop();
};

#endif // EVENTLOOP_H_INCLUDED
