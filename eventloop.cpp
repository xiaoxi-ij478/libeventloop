#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <utility>

#include <unistd.h>
#include <sys/select.h>

#include "eventloop.hpp"

EventLoop::EventLoop(unsigned event_loop_interval_us) :
    event_loop_interval_us(event_loop_interval_us),
    last_timer_id(0),
    last_event_id(0),
    event_queue(),
    timers(),
    manual_fds(),
    events()
{
}

unsigned EventLoop::add_event_listener(
    unsigned eventid,
    const std::function<void(unsigned, unsigned)> &func
)
{
    if (!events.count(eventid))
        events.emplace(
            eventid,
            std::unordered_map<unsigned, std::function<void(unsigned, unsigned)>>()
        );
    events.at(eventid).emplace(++last_event_id, func);
    return last_event_id;
}

void EventLoop::add_fd_listener(int fd, const std::function<void(int)> &func)
{
    manual_fds.emplace(fd, func);
}

void EventLoop::remove_event_listener(unsigned eventid, unsigned handle)
{
    if (!events.count(eventid))
        return;

    events.at(eventid).erase(handle);
}

void EventLoop::remove_fd_listener(int fd)
{
    if (!manual_fds.count(fd))
        return;

    manual_fds.erase(fd);
}

void EventLoop::trigger_event(unsigned eventid)
{
    event_queue.push(eventid);
}

void EventLoop::clear_timeout(unsigned handle)
{
    if (!timers.count(handle))
        return;

    timers.erase(handle);
}

void EventLoop::clear_interval(unsigned handle)
{
    clear_timeout(handle);
}

unsigned EventLoop::set_timeout(
    const std::chrono::milliseconds &timeout,
    const std::function<void(unsigned)> &func
)
{
    timers.emplace(
        ++last_timer_id,
        TimerInfo(std::chrono::steady_clock::now(), timeout, true, func)
    );
    return last_timer_id;
}

unsigned EventLoop::set_interval(
    const std::chrono::milliseconds &interval,
    const std::function<void(unsigned)> &func
)
{
    timers.emplace(
        ++last_timer_id,
        TimerInfo(std::chrono::steady_clock::now(), interval, false, func)
    );
    return last_timer_id;
}

void EventLoop::mainloop()
{
    while (true) {
        struct timeval tv = {
            event_loop_interval_us / 1000000,
            event_loop_interval_us % 1000000
        };
        int maxfd = 0;
        fd_set listen_fds;
        FD_ZERO(&listen_fds);

        for (const auto &i : manual_fds) {
            maxfd = std::max(maxfd, i.first);
            FD_SET(i.first, &listen_fds);
        }

        if (select(++maxfd, &listen_fds, nullptr, nullptr, &tv) == -1) {
            perror("select");
            return;
        }

        for (const auto &i : manual_fds)
            if (FD_ISSET(i.first, &listen_fds) && i.second)
                i.second(i.first);

        std::vector<int> to_be_removed_timers;

        for (auto &i : timers) {
            std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

            if (now - i.second.last_time >= i.second.interval) {
                i.second.last_time = now;
                i.second.callback(i.first);

                if (i.second.oneshot)
                    to_be_removed_timers.push_back(i.first);
            }
        }

        for (int i : to_be_removed_timers)
            clear_timeout(i);

        if (event_queue.empty())
            continue;

        if (events.count(event_queue.front()))
            for (const auto &i : events.at(event_queue.front()))
                i.second(event_queue.front(), i.first);

        event_queue.pop();
    }
}
