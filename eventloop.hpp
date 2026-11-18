#ifndef EVENTLOOP_H_INCLUDED
#define EVENTLOOP_H_INCLUDED

#include <chrono>
#include <unordered_map>
#include <functional>

struct TimerInfo {
    std::chrono::steady_clock::time_point last_time;
    std::chrono::milliseconds interval;
    bool oneshot;
    std::function<void(unsigned)> callback;
};

extern unsigned last_timer_id;
extern std::unordered_map<unsigned, TimerInfo> timers;

unsigned add_event_listener(unsigned eventid, const std::function<void(unsigned, unsigned)> &func);
void add_fd_listener(int fd, const std::function<void(int)> &func);
void remove_event_listener(unsigned eventid, unsigned handle);
void remove_fd_listener(int fd);
void trigger_event(unsigned eventid);

template<typename Rep, typename Period>
unsigned set_timeout(const std::chrono::duration<Rep, Period> &timeout, const std::function<void(unsigned)> &func)
{
    timers.emplace(
        ++last_timer_id,
        TimerInfo(std::chrono::steady_clock::now(), timeout, true, func)
    );
    return last_timer_id;
}

template<typename Rep, typename Period>
unsigned set_interval(const std::chrono::duration<Rep, Period> &interval, const std::function<void(unsigned)> &func)
{
    timers.emplace(
        ++last_timer_id,
        TimerInfo(std::chrono::steady_clock::now(), interval, false, func)
    );
    return last_timer_id;
}

void clear_timeout(unsigned handle);
void clear_interval(unsigned handle);
void mainloop();

#endif // EVENTLOOP_H_INCLUDED
