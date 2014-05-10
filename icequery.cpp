#include <cstdio>
#include <memory>       // unique_ptr
#include <ctime>        // clock_gettime()
#include <cstring>      // strerror()
#include <cstdint>      // uint32_t
#include <algorithm>    // transform()
#include <cctype>       // tolower
#include <vector>
#include <utility>      // pair

#include <poll.h>

#include <icecc/comm.h>
#include <icecc/logging.h>

#include <unicode/unistr.h>     // UnicodeString
#include <unicode/translit.h>   // Transliterator
#include <unicode/errorcode.h>  // ErrorCode

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
#define PRINT_WARN( format, args... )   PRINT_BASE( "\e[1;33m" format "\e[0m", ##args )

// Const macros

#define VERT_LINE_LO    "|"
#define CROSS_LO        "+"
#define HOR_LINE_LO     "-"

#define VERT_LINE_HI    "│"
#define CROSS_HI        "┼"
#define HOR_LINE_HI     "─"

#define TICK_HI         "√"
#define TICK_LO         "X"
#define NO_TICK_HI      ""
#define NO_TICK_LO      ""

// Types

enum Alignment
{
    Left,
    Right,
    Center
};

// Global variables

std::string netName = "ICECREAM";
int timeout = 2000;
std::string schedIp = "";
int schedPort = 0;

bool quiet = false;
bool brief = true;

bool noTable = true;
bool plain = false;
bool lowAscii = false;

bool filterOffline = false;
bool filterNoRemote = false;

// Utility functions

long getTimestamp()
{
    static struct timespec ts;

    clock_gettime( CLOCK_MONOTONIC_RAW, &ts );

    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

std::string toLower( std::string&& str )
{
    std::transform( str.cbegin(), str.cend(), str.begin(), tolower );

    return std::move( str );
}

std::string& toLower( std::string& str )
{
    std::transform( str.cbegin(), str.cend(), str.begin(), tolower );

    return str;
}

std::string renderTable( const std::vector<std::pair<Alignment, std::string>>& header, const std::vector<std::string>& strings, bool plain, bool useLowAscii )
{
    auto columnCount = header.size();
    auto rowCount = strings.size() / columnCount;

    std::vector<std::size_t> colMaxLens( columnCount );

    std::unique_ptr<Transliterator> trans;
    std::vector<std::string> data( columnCount * ( rowCount + 1 ) );
    std::vector<std::size_t> lens( columnCount * ( rowCount + 1 ) );

    // Init Transliterator
    if( useLowAscii )
    {
        ErrorCode errorCode;
        trans.reset( Transliterator::createInstance( "Latin-ASCII", UTRANS_FORWARD, errorCode ) );

        if( errorCode.isFailure() )
        {
            PRINT_ERR( "Transliterator::createInstance(): %s\n", errorCode.errorName() );
            trans.reset();
        }
    }

    // Convert to UnicodeString, transliterate if needed, determine column lengths and store back a string
    for( std::size_t c = 0; c < columnCount; ++c )
    {
        for( std::size_t r = 0; r < rowCount + 1; ++r )
        {
            const std::string& origStr = r == 0 ? header[c].second : strings[columnCount * (r - 1) + c];
            UnicodeString uniStr( origStr.c_str() );

            std::size_t& maxLen = colMaxLens[c];
            std::size_t& len = lens[columnCount * r + c];
            std::string& str = data[columnCount * r + c];

            if( trans )
            {
                trans->transliterate( uniStr );

                std::unique_ptr<char> buff( new char[uniStr.extract( 0, uniStr.length(), static_cast<char*>( NULL ) )] );

                uniStr.extract( 0, uniStr.length(), buff.get() );
                str.assign( buff.get() );
            }
            else
            {
                str.assign( origStr );
            }

            len = static_cast<std::size_t>( uniStr.length() );

            // Add 1 char margin to the first and last column
            if( !plain )
            {
                if( c == 0 )
                {
                    str.insert( 0, 1, ' ' );
                    ++len;
                }
                else if( c == columnCount - 1 )
                {
                    str.append( 1, ' ' );
                    ++len;
                }
            }

            maxLen = std::max( maxLen, len );
        }
    }

    // Pad the strings
    for( std::size_t c = 0; c < columnCount; ++c )
    {
        const std::size_t& maxLen = colMaxLens[c];
        const Alignment align = header[c].first;

        for( std::size_t r = 0; r < rowCount + 1; ++r )
        {
            std::string& str = data[columnCount * r + c];
            const std::size_t& strLen = lens[columnCount * r + c];

            if( strLen < maxLen )
            {
                std::size_t lenDiff = maxLen - strLen;

                switch( align )
                {
                    case Left:
                        str.append( lenDiff, ' ' );
                        break;

                    case Right:
                        str.insert( 0, lenDiff, ' ' );
                        break;

                    case Center:
                        str.insert( 0, lenDiff / 2, ' ' ).append( lenDiff - lenDiff / 2, ' ' );
                        break;
                }
            }
        }
    }

    // Render the table
    std::string table;

    for( std::size_t r = 0; r < rowCount + 1; ++r )
    {
        for( std::size_t c = 0; c < columnCount; ++c )
        {
            if( c > 0 )
            {
                if( plain )
                {
                   table.append( " " );
                }
                else
                {
                    if( useLowAscii )
                    {
                        table.append( " " VERT_LINE_LO " " );
                    }
                    else
                    {
                        table.append( " " VERT_LINE_HI " " );
                    }
                }
            }

            table.append( data[columnCount * r + c] );
        }

        table.append( "\n" );

        // Draw the horizontal line below the header
        if( r == 0 && !plain )
        {
            for( std::size_t c = 0; c < columnCount; ++c )
            {
                if( c > 0 )
                {
                    if( useLowAscii )
                    {
                        table.append( HOR_LINE_LO CROSS_LO HOR_LINE_LO );
                    }
                    else
                    {
                        table.append( HOR_LINE_HI CROSS_HI HOR_LINE_HI );
                    }
                }

                for( std::size_t i = 0; i < colMaxLens[c]; ++i )
                {
                    table.append( useLowAscii ? HOR_LINE_LO : HOR_LINE_HI );
                }
            }

            table.append( "\n" );
        }
    }

    return std::move( table );
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
            res.reset();
        }

        return std::move( res );
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

    const std::string& platform() const
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
    if( quiet )
    {
        // This somehow suppresses all the internal debug messages sent onto stderr by icecc
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

    std::vector<std::unique_ptr<NodeInfo>> nodes;
    uint32_t hostIdMax = 0;

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

            if( nodeInfo && nodeInfo->hostId() > hostIdMax)
            {
                // Keep track of highest hostId in case the scheduler sends multiple copies of the same hostInfos
                hostIdMax = nodeInfo->hostId();
                nodes.push_back( std::move( nodeInfo ) );
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

    if( nodes.empty() ||
        std::all_of( nodes.cbegin(), nodes.cend(), [] ( const auto& node ) -> bool
        {
            return ( filterOffline && node->isOffline() ) || ( filterNoRemote && node->noRemote() );
        } ) )
    {
        PRINT_ERR( "No useful data received.\n" );

        return 1;
    }
    else
    {
        std::uint32_t coreCount = std::accumulate( nodes.cbegin(), nodes.cend(), static_cast<std::uint32_t>( 0 ), [] ( std::uint32_t count, const auto& node )
        {
            if( !node->noRemote() && !node->isOffline() )
            {
                return count + node->maxJobs();
            }
            else
            {
                return count;
            }
        } );

        std::uint32_t nodeCount = std::count_if( nodes.cbegin(), nodes.cend(), [] ( const auto& node ) { return !node->isOffline(); } );

        if( brief )
        {
            printf( "%u\n", coreCount );
        }
        else
        {
            std::vector<std::pair<Alignment, std::string>> header {
                { Alignment::Right , "Node #" },
                { Alignment::Center, "Offline?" },
                { Alignment::Center, "No remote?" },
                { Alignment::Left  , "Name" },
                { Alignment::Left  , "IP" },
                { Alignment::Right , "Cores" },
                { Alignment::Left  , "Platform" }
            };

            std::vector<std::string> strings;

            for( const auto& node : nodes )
            {
                if( ( !filterOffline || !node->isOffline() ) && ( !filterNoRemote || !node->noRemote() ) )
                {
                    strings.emplace_back( std::to_string( node->hostId() ) );
                    strings.emplace_back( node->isOffline() ? ( lowAscii ? TICK_LO : TICK_HI ) : ( lowAscii ? NO_TICK_LO : NO_TICK_HI ) );
                    strings.emplace_back( node->noRemote()  ? ( lowAscii ? TICK_LO : TICK_HI ) : ( lowAscii ? NO_TICK_LO : NO_TICK_HI ) );
                    strings.push_back( node->name() );
                    strings.push_back( node->ip() );
                    strings.emplace_back( std::to_string( node->maxJobs() ) );
                    strings.push_back( node->platform() );
                }
            }

            if( !noTable )
            {
               printf( "\n%s\n", renderTable( header, strings, plain, lowAscii ).c_str() );
            }

            printf( "%u nodes, %u cores total.\n", nodeCount, coreCount );
        }

        return 0;
    }
}
