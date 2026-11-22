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
        using EventCallbackType = std::function<void(unsigned, unsigned, std::shared_ptr<EventData>)>;
        using TimerCallbackType = std::function<void(int)>;

        unsigned last_event_handle;
        int event_queue_pipe_fd[2];
        std::unordered_set<int> oneshot_timer_fds;
        // arg1: fd
        std::unordered_map<int, TimerCallbackType> timer_fds, manual_fds;
        // arg1: event id, arg2: event handle, arg3: data
        std::unordered_map<unsigned, std::unordered_map<unsigned, EventCallbackType>> events;
        std::queue<std::shared_ptr<EventData>> events_data;

    public:
        EventLoop();
        // we do not support copying an event loop
        // otherwise multiple event loop calling same callbacks will cause problems
        EventLoop(const EventLoop &other) = delete;
        EventLoop(EventLoop &&other);
        EventLoop &operator=(const EventLoop &other) = delete;
        EventLoop &operator=(EventLoop &&other);
        ~EventLoop();

        unsigned add_event_listener(
            unsigned eventid,
            const EventCallbackType &func
        );
        void add_fd_listener(int fd, const TimerCallbackType &func);
        int set_timeout(
            const std::chrono::nanoseconds &timeout,
            const TimerCallbackType &func
        );
        int set_interval(
            const std::chrono::nanoseconds &interval,
            const TimerCallbackType &func
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
