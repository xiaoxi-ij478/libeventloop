#include <cstdint>
#include <algorithm>
#include <iterator>
#include <utility>
#include <stdexcept>

#include <unistd.h>
#include <sys/select.h>
#include <sys/timerfd.h>

#include "eventloop.hpp"

EventLoop::EventLoop() :
    last_event_handle(0),
    event_queue_pipe_fd(),
    oneshot_timer_fds(),
    timer_fds(),
    manual_fds(),
    events(),
    events_data()
{
    if (pipe(event_queue_pipe_fd) == -1)
        throw std::system_error(std::error_code(errno, std::general_category()));
}

unsigned EventLoop::add_event_listener(
    unsigned eventid,
    const std::function<void(unsigned, unsigned, std::shared_ptr<EventData>)> &func
)
{

    if (!events.count(eventid))
        events.emplace(eventid, decltype(events.cbegin()->second)());

    events.at(eventid).emplace(++last_event_handle, func);
    return last_event_handle;
}

void EventLoop::add_fd_listener(int fd, const std::function<void(int)> &func)
{
    manual_fds.emplace(fd, func);
}

int EventLoop::set_timeout(
    const std::chrono::nanoseconds &timeout,
    const std::function<void(int)> &func
)
{
    int timerfd = timerfd_create(CLOCK_MONOTONIC, 0);

    if (timerfd == -1)
        throw std::system_error(std::error_code(errno, std::general_category()));

    struct itimerspec it = {
        { 0, 0 },
        { timeout.count() / 1000000000, timeout.count() % 1000000000 }
    };

    if (timerfd_settime(timerfd, 0, &it, nullptr) == -1)
        throw std::system_error(std::error_code(errno, std::general_category()));

    timer_fds.emplace(timerfd, func);
    oneshot_timer_fds.emplace(timerfd);
    return timerfd;
}

int EventLoop::set_interval(
    const std::chrono::nanoseconds &interval,
    const std::function<void(int)> &func
)
{
    int timerfd = timerfd_create(CLOCK_MONOTONIC, 0);

    if (timerfd == -1)
        throw std::system_error(std::error_code(errno, std::general_category()));

    struct itimerspec it = {
        { interval.count() / 1000000000, interval.count() % 1000000000 },
        { interval.count() / 1000000000, interval.count() % 1000000000 }
    };

    if (timerfd_settime(timerfd, 0, &it, nullptr) == -1)
        throw std::system_error(std::error_code(errno, std::general_category()));

    timer_fds.emplace(timerfd, func);
    return timerfd;
}

int EventLoop::after_idle(const std::function<void()> &func)
{
    return set_timeout(std::chrono::nanoseconds(1), [&func](int) {
        func();
    });
}

void EventLoop::remove_event_listener(unsigned handle)
{

    for (auto &i : events) {
        if (!i.second.count(handle))
            continue;

        i.second.erase(handle);
        break;
    }
}

void EventLoop::remove_fd_listener(int fd)
{
    manual_fds.erase(fd);
}

void EventLoop::trigger_event(unsigned eventid, const std::shared_ptr<EventData> &data)
{
    if (write(event_queue_pipe_fd[1], &eventid, sizeof(eventid)) == -1)
        throw std::system_error(std::error_code(errno, std::general_category()));

    events_data.push(data);
}

void EventLoop::clear_timeout(int fd)
{

    if (!timer_fds.count(fd))
        return;

    close(fd);
    oneshot_timer_fds.erase(fd);
    timer_fds.erase(fd);
}

void EventLoop::clear_interval(int fd)
{
    clear_timeout(fd);
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
            throw std::system_error(std::error_code(errno, std::general_category()));

        for (auto it = manual_fds.cbegin(); it != manual_fds.cend();) {
            auto next = std::next(it);

            if (FD_ISSET(it->first, &listen_fds) && it->second) {
                it->second(it->first);
            }

            it = next;
        }

        for (auto it = timer_fds.cbegin(); it != timer_fds.cend();) {
            auto next = std::next(it);

            if (FD_ISSET(it->first, &listen_fds) && it->second) {
                uint64_t a;
                read(it->first, &a, sizeof(a));
                // to prevent callback from closing fd prematurely, call it after getting required info
                it->second(it->first);

                if (oneshot_timer_fds.count(it->first))
                    clear_timeout(it->first);
            }

            it = next;
        }

        if (!FD_ISSET(event_queue_pipe_fd[0], &listen_fds))
            continue;

        unsigned pending_events = events_data.size();
        std::vector<unsigned> pending_eventids;
        pending_eventids.resize(pending_events);

        if (
            read(
                event_queue_pipe_fd[0],
                pending_eventids.data(),
                sizeof(unsigned) * pending_events
            ) == -1
        )
            throw std::system_error(std::error_code(errno, std::general_category()));

        for (unsigned i : pending_eventids) {

            for (auto it = events.at(i).cbegin(); it != events.at(i).cend();) {
                auto next = std::next(it);
                it->second(i, it->first, events_data.front());
                it = next;
            }

            events_data.pop();
        }
    }
}
