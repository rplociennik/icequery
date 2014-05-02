#include <cstdio>
#include <memory>       // unique_ptr
#include <ctime>        // clock_gettime()
#include <cstring>      // strerror()
#include <cstdint>      // uint32_t
#include <algorithm>    // transform()
#include <cctype>       // tolower
#include <list>

#include <poll.h>

#include <icecc/comm.h>
#include <icecc/logging.h>

// Utility macros

#define PRINT_BASE( format, args... ) \
    do \
    { \
        if( !quiet ) \
        { \
            fprintf( stderr, format, ##args ); \
        } \
    } \
    while( 0 )

#define PRINT_ERR( format, args... )    PRINT_BASE( "\e[31m" format "\e[0m", ##args )
#define PRINT_WARN( format, args... )   PRINT_BASE( "\e[33m" format "\e[0m", ##args )
#define PRINT_INFO( format, args... )   PRINT_BASE( "\e[32m" format "\e[0m", ##args )

// Utility functions

long getTimestamp()
{
    static struct timespec ts;

    clock_gettime( CLOCK_MONOTONIC_RAW, &ts );

    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

std::string toLower( std::string&& str )
{
    std::transform( str.begin(), str.end(), str.begin(), tolower );

    return std::move( str );
}

std::string& toLower( std::string& str )
{
    std::transform( str.begin(), str.end(), str.begin(), tolower );

    return str;
}

// Classes

class NodeInfo
{
public:
    static std::unique_ptr<NodeInfo> create( uint32_t hostId, const std::string& stats )
    {
        if( !hostId )
        {
            return std::unique_ptr<NodeInfo>();
        }

        std::unique_ptr<NodeInfo> res( new NodeInfo( hostId ) );

        std::string::size_type linePos = 0;

        while( true )
        {
            auto nextLinePos = stats.find( '\n', linePos );
            auto colonPos = stats.find( ':', linePos );

            if( colonPos != std::string::npos && !( nextLinePos != std::string::npos && colonPos > nextLinePos ) )
            {
                std::string name = std::move( toLower( stats.substr( linePos, colonPos - linePos ) ) );
                std::string value = std::move( stats.substr( colonPos + 1, nextLinePos == std::string::npos ? std::string::npos : nextLinePos - ( colonPos + 1 ) ) );

                if( name == "name" )
                {
                    res->m_name = std::move( value );
                }
                else if( name == "ip" )
                {
                    res->m_ip = std::move( value );
                }
                else if( name == "maxjobs" )
                {
                    res->m_maxJobs = std::stoi( value );
                }
                else if( name == "noremote" )
                {
                    res->m_noRemote = ( toLower( value ) == "true" );
                }
                else if( name == "state" )
                {
                    res->m_offline = ( toLower( value ) == "offline" );
                }
                else if( name == "platform" )
                {
                    res->m_platform = std::move( value );
                }
            }

            linePos = nextLinePos;

            if( nextLinePos == std::string::npos )
            {
                break;
            }
            else
            {
                ++linePos;

                if( linePos > stats.length() )
                {
                    break;
                }
            }
        }

        if( !res->isValid() )
        {
            res.reset( nullptr );
        }

        return res;
    }

    uint32_t hostId() const
    {
        return m_hostId;
    }

    const std::string& name() const
    {
        return m_name;
    }

    const std::string& ip() const
    {
        return m_ip;
    }

    uint32_t maxJobs() const
    {
        return m_maxJobs;
    }

    bool noRemote() const
    {
        return m_noRemote;
    }

    bool isOffline() const
    {
        return m_offline;
    }

    const std::string platform() const
    {
        return m_platform;
    }

private:
    NodeInfo( uint32_t hostId )
        : m_hostId( hostId )
    {
    }

    bool isValid() const
    {
        return m_hostId &&
               !m_name.empty() &&
               !m_ip.empty() &&
               m_maxJobs &&
               !m_platform.empty();
    }

private:
    uint32_t m_hostId = 0;
    std::string m_name;
    std::string m_ip;
    uint32_t m_maxJobs = 0;
    bool m_noRemote = false;
    bool m_offline = false;
    std::string m_platform;
};

// And the entry point...

int main( void )
{
    const char* netName = "ICECREAM";
    const int timeout = 2000;
    const char* schedIp = "";
    const int schedPort = 0;
    const bool quiet = false;
    const bool brief = false;

    // This somehow suppresses all the internal debug messages sent onto stderr by icecc
    if( quiet )
    {
        reset_debug( 0 );
    }

    std::unique_ptr<MsgChannel> channel;
    std::unique_ptr<DiscoverSched> discover( new DiscoverSched( netName, timeout, schedIp, schedPort ) );

    long channelTimestamp = getTimestamp();

    do
    {
        channel.reset( discover->try_get_scheduler() );
    }
    while( !channel && ( getTimestamp() - channelTimestamp ) < timeout );

    if( !channel )
    {
        if( !discover->timed_out() )
        {
            PRINT_ERR( "Timed out while trying to connect to the scheduler.\n" );
        }

        return 1;
    }

    channel->setBulkTransfer();
    discover.reset( nullptr );

    if( !channel->send_msg( MonLoginMsg() ) )
    {
        PRINT_ERR( "MsgChannel::send_msg(): Scheduler rejected the MonLoginMsg message.\n" );

        return 1;
    }

    struct pollfd pollData { channel->fd, POLLIN | POLLPRI, 0 };

    int pollRes = poll( &pollData, 1, timeout );

    if( pollRes < 0 )
    {
        int lastErrno = errno;

        PRINT_ERR( "poll(): (%d) %s\n", lastErrno, strerror( lastErrno ) );

        return 1;
    }
    else if( pollRes == 0 )
    {
        PRINT_ERR( "poll(): Timed out white awaiting response from the scheduler.\n" );

        return 1;
    }

    std::list<std::unique_ptr<NodeInfo> > nodeList;

    while( !channel->read_a_bit() || channel->has_msg() )
    {
        std::unique_ptr<Msg> msg( channel->get_msg() );

        if( !msg )
        {
            PRINT_ERR( "MsgChannel::get_msg(): No message received from the scheduler.\n" );

            return 1;
        }

        if( msg->type == M_MON_STATS )
        {
            const MonStatsMsg* statsMsg = dynamic_cast<MonStatsMsg*>(msg.get());

            auto nodeInfo = std::move( NodeInfo::create( statsMsg->hostid, statsMsg->statmsg ) );

            if( nodeInfo )
            {
                nodeList.push_back( std::move( nodeInfo ) );
            }

            continue;
        }
        else if( msg->type == M_END )
        {
            PRINT_ERR( "Received EndMsg. Scheduler has quit.\n" );

            return 1;
        }
        else
        {
            PRINT_WARN( "Unknown message type: %d\n", msg->type );

            continue;
        }
    }

    if( nodeList.empty() )
    {
        PRINT_ERR( "No useful data received.\n" );

        return 1;
    }
    else
    {
        if( brief )
        {
            uint32_t jobCount = 0;

            for( const auto& node : nodeList )
            {
                if( !node->noRemote() && !node->isOffline() )
                {
                    jobCount += node->maxJobs();
                }
            }

            printf( "%u\n", jobCount );
        }
        else
        {
            // <!> Dummy

            PRINT_INFO( "%zu nodes found.\n", nodeList.size() );
        }

        return 0;
    }
}
