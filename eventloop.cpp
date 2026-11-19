#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <utility>
#include <stdexcept>

#include <unistd.h>
#include <sys/select.h>
#include <sys/timerfd.h>

#include "eventloop.hpp"

EventLoop::EventLoop() :
    event_queue_pipe_fd(),
    pending_events(0),
    timer_fds(),
    manual_fds(),
    events()
{
    if (pipe(event_queue_pipe_fd) == -1)
        throw std::system_error(std::error_code(errno, std::system_category()));
}

bool EventLoop::add_event_listener(
    unsigned eventid,
    const std::function<void(unsigned)> &func
)
{
    return events.emplace(eventid, func).second;
}

bool EventLoop::add_fd_listener(int fd, const std::function<void(int)> &func)
{
    return manual_fds.emplace(fd, func).second;
}

int EventLoop::set_timeout(
    const std::chrono::nanoseconds &timeout,
    const std::function<void(int)> &func
)
{
    int timerfd = timerfd_create(CLOCK_MONOTONIC, 0);

    if (timerfd == -1)
        throw std::system_error(std::error_code(errno, std::system_category()));

    struct itimerspec it = {
        { 0, 0 },
        { timeout.count() / 1000000000, timeout.count() % 1000000000 }
    };

    if (timerfd_settime(timerfd, 0, &it, nullptr) == -1)
        throw std::system_error(std::error_code(errno, std::system_category()));

    timer_fds.emplace(timerfd, func);
    return timerfd;
}

int EventLoop::set_interval(
    const std::chrono::nanoseconds &interval,
    const std::function<void(int)> &func
)
{
    int timerfd = timerfd_create(CLOCK_MONOTONIC, 0);

    if (timerfd == -1)
        throw std::system_error(std::error_code(errno, std::system_category()));

    struct itimerspec it = {
        { interval.count() / 1000000000, interval.count() % 1000000000 },
        { interval.count() / 1000000000, interval.count() % 1000000000 }
    };

    if (timerfd_settime(timerfd, 0, &it, nullptr) == -1)
        throw std::system_error(std::error_code(errno, std::system_category()));

    timer_fds.emplace(timerfd, func);
    return timerfd;
}

bool EventLoop::remove_event_listener(unsigned eventid)
{
    return events.erase(eventid);
}

bool EventLoop::remove_fd_listener(int fd)
{
    return manual_fds.erase(fd);
}

void EventLoop::trigger_event(unsigned eventid)
{
    if (write(event_queue_pipe_fd[1], &eventid, sizeof(eventid)) == -1)
        throw std::system_error(std::error_code(errno, std::system_category()));

    pending_events++;
}

bool EventLoop::clear_timeout(int handle)
{
    if (!timer_fds.count(handle))
        return false;

    close(handle);
    return timer_fds.erase(handle);
}

bool EventLoop::clear_interval(int handle)
{
    return clear_timeout(handle);
}

void EventLoop::mainloop()
{
    while (true) {
        int maxfd = 0;
        fd_set listen_fds;
        FD_ZERO(&listen_fds);

        for (const auto &i : manual_fds) {
            maxfd = std::max(maxfd, i.first);
            FD_SET(i.first, &listen_fds);
        }

        for (const auto &i : timer_fds) {
            maxfd = std::max(maxfd, i.first);
            FD_SET(i.first, &listen_fds);
        }

        maxfd = std::max(maxfd, event_queue_pipe_fd[0]);
        FD_SET(event_queue_pipe_fd[0], &listen_fds);

        if (select(++maxfd, &listen_fds, nullptr, nullptr, nullptr) == -1)
            throw std::system_error(std::error_code(errno, std::system_category()));

        // *INDENT-OFF*
        std::vector<std::pair<int, std::function<void(int)>>> should_call_cb_fds;
        // *INDENT-ON*
        for (const auto &i : manual_fds)
            if (FD_ISSET(i.first, &listen_fds) && i.second)
                should_call_cb_fds.push_back(i);

        for (const auto &i : timer_fds)
            if (FD_ISSET(i.first, &listen_fds) && i.second) {
                should_call_cb_fds.push_back(i);
                uint64_t a;
                read(i.first, &a, sizeof(a));
            }

        for (const auto &i : should_call_cb_fds)
            i.second(i.first);

        if (!FD_ISSET(event_queue_pipe_fd[0], &listen_fds))
            continue;

        std::vector<unsigned> pending_eventids;
        pending_eventids.resize(pending_events);

        if (
            read(
                event_queue_pipe_fd[0],
                pending_eventids.data(),
                sizeof(unsigned) * pending_events
            ) == -1
        )
            throw std::system_error(std::error_code(errno, std::system_category()));

        pending_events = 0;

        for (unsigned i : pending_eventids)
            events.at(i)(i);
    }
}
