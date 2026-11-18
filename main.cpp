#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <queue>
#include <thread>
#include <utility>

#include <unistd.h>
#include <sys/select.h>

#include "eventloop.hpp"

static std::queue<unsigned> event_queue;
static unsigned last_event_id = 0;
unsigned last_timer_id = 0;
std::unordered_map<unsigned, TimerInfo> timers;
static std::unordered_map<int, std::function<void(int)>> manual_fds;
// *INDENT-OFF*
static std::unordered_map<
    unsigned, // event id
    std::unordered_map<
        unsigned, // event handle
        std::function<void(unsigned, unsigned)> // callback(event id, event handle)
    >
> events;
// *INDENT-ON*

unsigned add_event_listener(unsigned eventid, const std::function<void(unsigned, unsigned)> &func)
{
    if (!events.count(eventid))
        events.emplace(eventid, std::unordered_map<unsigned, std::function<void(unsigned, unsigned)>>());

    events.at(eventid).emplace(++last_event_id, func);
    return last_event_id;
}

void add_fd_listener(int fd, const std::function<void(int)> &func)
{
    manual_fds.emplace(fd, func);
}

void remove_event_listener(unsigned eventid, unsigned handle)
{
    if (!events.count(eventid))
        return;

    events.at(eventid).erase(handle);
}

void remove_fd_listener(int fd)
{
    if (!manual_fds.count(fd))
        return;

    manual_fds.erase(fd);
}

void trigger_event(unsigned eventid)
{
    event_queue.push(eventid);
}

void clear_timeout(unsigned handle)
{
    if (!timers.count(handle))
        return;

    timers.erase(handle);
}

void clear_interval(unsigned handle)
{
    clear_timeout(handle);
}

void mainloop()
{
    while (true) {
        struct timeval tv = { 0, 1000 };
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

        if (!events.count(event_queue.front()))
            break;

        for (const auto &i : events.at(event_queue.front()))
            i.second(event_queue.front(), i.first);

        event_queue.pop();
    }
}
