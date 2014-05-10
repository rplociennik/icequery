#include <cstdio>
#include <memory>       // unique_ptr
#include <algorithm>
#include <vector>

#include <ctime>        // clock_gettime()
#include <cstring>      // strerror()
#include <cstdint>      // uint32_t
#include <cctype>       // tolower

#include <poll.h>
#include <getopt.h>

#include <icecc/comm.h>
#include <icecc/logging.h>

#include <unicode/unistr.h>     // icu::UnicodeString
#include <unicode/translit.h>   // icu::Transliterator
#include <unicode/errorcode.h>  // icu::ErrorCode

// Utility macros

#define PRINT_BASE( format, args... ) \
    do \
    { \
        if( !veryQuiet ) \
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

// Exit codes

#define EXIT_OK             0

#define EXIT_INVALID_ARGS   1
#define EXIT_CONNECTION_ERR 2
#define EXIT_NO_DATA        3

// Types

enum Alignment
{
    Left,
    Right,
    Center
};

// Global variables

std::string netName;
int timeout = 2000;
std::string schedAddr;
uint16_t schedPort = 0;

bool quiet = false;
bool veryQuiet = false;
bool brief = false;

bool noTable = false;
bool plain = false;
bool ascii = false;

bool noOffline = false;
bool noNoRemote = false;

// Consts

const char* UsageStr = R"usage(
usage: %s [options...]

General options:

 -h, --help            : displays this info

Connection options:

 -n, --net-name=<name> : net name to use when connecting to the scheduler
 -t, --timeout=<msecs> : timeout for establishing connection and retrieving
                         data (default: 2000)
     --addr=<address>  : scheduler address for avoiding broadcasting and
                         attempting to connect directly
     --port=<port>     : scheduler port for direct connection

General output options:

 -q, --quiet           : suppress any icecc debug messages sent to stderr
 -Q, --very-quiet      : suppress all error messages entirely
 -b, --brief           : on success return only a single numeric value
                         representing the number of available cores
                         (implies --very-quiet, --no-table)

Table options:

 -P, --plain           : print the table without any borders
 -A, --ascii           : produce only ASCII output by displaying table borders
                         as low order ASCII characters and performing
                         transliteration on any names encountered
 -T, --no-table        : do not print the table entirely, only a summary on
                         success

 --no-offline  [*]     : do not include offline nodes in the table
 --no-noremote [*]     : do not include 'no remote' nodes in the table

 [*] Selected options affect the display of the table only, as neither offline
     nor 'no remote' nodes are taken into account when calculating totals.

Exit codes:

 0 : No errors occurred

 1 : Command-line error
 2 : Connection error
 3 : No useful data retrieved
)usage";

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

public:
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

class ColumnHeader
{
public:
    ColumnHeader( Alignment alignment, bool treatAsUnicode, const std::string& name )
        : m_alignment( alignment )
        , m_treatAsUnicode( treatAsUnicode )
        , m_name( name )
    {
    }

    ColumnHeader( Alignment alignment, bool treatAsUnicode, const std::string&& name )
        : m_alignment( alignment )
        , m_treatAsUnicode( treatAsUnicode )
        , m_name( std::move( name ) )
    {
    }

public:
    Alignment alignment() const
    {
        return m_alignment;
    }

    bool treatAsUnicode() const
    {
        return m_treatAsUnicode;
    }

    const std::string& name() const
    {
        return m_name;
    }

private:
    Alignment m_alignment;
    bool m_treatAsUnicode;
    std::string m_name;
};

// Other functions

std::string renderTable( const std::vector<ColumnHeader>& header, const std::vector<std::string>& strings, bool plain, bool ascii )
{
    auto columnCount = header.size();
    auto rowCount = strings.size() / columnCount;

    std::vector<std::size_t> colMaxLens( columnCount );

    std::unique_ptr<icu::Transliterator> trans;
    std::vector<std::string> data( columnCount * ( rowCount + 1 ) );
    std::vector<std::size_t> lens( columnCount * ( rowCount + 1 ) );

    // Init Transliterator
    if( ascii )
    {
        icu::ErrorCode errorCode;
        trans.reset( icu::Transliterator::createInstance( "Latin-ASCII", UTRANS_FORWARD, errorCode ) );

        if( errorCode.isFailure() )
        {
            PRINT_ERR( "icu::Transliterator::createInstance(): %s\n", errorCode.errorName() );
            trans.reset();
        }
    }

    // If needed, treat as UnicodeString and possibly transliterate,
    // otherwise just determine column length and store back a string
    for( std::size_t c = 0; c < columnCount; ++c )
    {
        std::size_t& maxLen = colMaxLens[c];

        for( std::size_t r = 0; r < rowCount + 1; ++r )
        {
            std::size_t& len = lens[columnCount * r + c];
            std::string& res = data[columnCount * r + c];

            const std::string& origStr = r == 0 ? header[c].name() : strings[columnCount * (r - 1) + c];

            if( header[c].treatAsUnicode() )
            {
                icu::UnicodeString uniStr( origStr.c_str() );

                if( trans )
                {
                    trans->transliterate( uniStr );

                    std::unique_ptr<char> buff( new char[uniStr.extract( 0, uniStr.length(), static_cast<char*>( NULL ) )] );

                    uniStr.extract( 0, uniStr.length(), buff.get() );
                    res.assign( buff.get() );
                }
                else
                {
                    res.assign( origStr );
                }

                len = static_cast<std::size_t>( uniStr.length() );
            }
            else
            {
                res.assign( origStr );
                len = res.length();
            }

            // Add 1 char margin to the first and last column
            if( !plain )
            {
                if( c == 0 )
                {
                    res.insert( 0, 1, ' ' );
                    ++len;
                }
                else if( c == columnCount - 1 )
                {
                    res.append( 1, ' ' );
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
        const Alignment align = header[c].alignment();

        for( std::size_t r = 0; r < rowCount + 1; ++r )
        {
            std::string& str = data[columnCount * r + c];
            const std::size_t& len = lens[columnCount * r + c];

            if( len < maxLen )
            {
                std::size_t lenDiff = maxLen - len;

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

    std::size_t lensSum = std::accumulate( lens.cbegin(), lens.cend(), static_cast<std::size_t>( 0 ) );
    table.reserve( ( lensSum + ( lensSum - 1 ) + 1 ) * ( plain ? 1 : 3 ) + 1 );

    for( std::size_t r = 0; r < rowCount + 1; ++r )
    {
        for( std::size_t c = 0; c < columnCount; ++c )
        {
            if( c > 0 )
            {
                if( plain )
                {
                   table.append( 1, ' ' );
                }
                else
                {
                    if( ascii )
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

        table.append( 1, '\n' );

        // Draw the horizontal line below the header
        if( r == 0 && !plain )
        {
            for( std::size_t c = 0; c < columnCount; ++c )
            {
                if( c > 0 )
                {
                    if( ascii )
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
                    table.append( ascii ? HOR_LINE_LO : HOR_LINE_HI );
                }
            }

            table.append( 1, '\n' );
        }
    }

    return std::move( table );
}

// And the entry point...

int main( int argc, char** argv )
{
    // Command-line option parsing

    // Disable built-in error messages
    opterr = 0;

    while( true )
    {
        static struct option options[] = {
            { "help",        no_argument,       0, 'h' },

            { "net-name",    required_argument, 0, 'n' },
            { "timeout",     required_argument, 0, 't' },
            { "addr",        required_argument, 0,  1  },
            { "port",        required_argument, 0,  2  },

            { "quiet",       no_argument,       0, 'q' },
            { "very-quiet",  no_argument,       0, 'Q' },
            { "brief",       no_argument,       0, 'b' },

            { "no-table",    no_argument,       0, 'T' },
            { "plain",       no_argument,       0, 'P' },
            { "ascii",       no_argument,       0, 'A' },

            { "no-offline",  no_argument,       0,  3  },
            { "no-noremote", no_argument,       0,  4  },
            { 0,             0,                 0,  0  }
        };

        int optRes = getopt_long( argc, argv, ":hn:t:qQbTPA", options, NULL );

        if( optRes == -1 )
        {
            break;
        }

        switch( optRes )
        {
            case 'h':
                fprintf( stderr, UsageStr, argv[0] );
                return EXIT_INVALID_ARGS;

            case '?':
                PRINT_ERR( "Unknown/ambiguous option '%s'. Try '--help'.\n", argv[optind - 1] );
                return EXIT_INVALID_ARGS;

            case ':':
                PRINT_ERR( "Missing argument for '%s'.\n", argv[optind - 1] );
                return EXIT_INVALID_ARGS;

            case 'n':
                netName.assign( optarg );
                break;

            case 't':
                if( sscanf( optarg, "%u", &timeout ) != 1 )
                {
                    PRINT_ERR( "Invalid argument for '%s'.\n", argv[optind - 1] );
                    return EXIT_INVALID_ARGS;
                }
                break;

            case 'q':
                quiet = true;
                break;

            case 'Q':
                quiet = true;
                veryQuiet = true;
                break;

            case 'b':
                brief = true;
                quiet = true;
                veryQuiet = true;
                noTable = true;
                break;

            case 'T':
                noTable = true;
                break;

            case 'P':
                plain = true;
                break;

            case 'A':
                ascii = true;
                break;

            case 1: // addr
                schedAddr.assign( optarg );
                break;

            case 2: // port
                if( sscanf( optarg, "%hu", &schedPort ) != 1 )
                {
                    PRINT_ERR( "Invalid argument for '%s'.\n", argv[optind - 1] );
                    return EXIT_INVALID_ARGS;
                }
                break;

            case 3: // no-offline
                noOffline = true;
                break;

            case 4: // no-noremote
                noNoRemote = true;
                break;
        }
    }

    // Getting to it...

    if( quiet )
    {
        // This somehow suppresses all the internal debug messages sent onto stderr by icecc
        reset_debug( 0 );
    }

    std::unique_ptr<MsgChannel> channel;
    std::unique_ptr<DiscoverSched> discover( new DiscoverSched( netName, timeout, schedAddr, schedPort ) );

    long channelTimestamp = getTimestamp();

    do
    {
        channel.reset( discover->try_get_scheduler() );

        if( discover->timed_out() )
        {
            return EXIT_CONNECTION_ERR;
        }
    }
    while( !channel && ( getTimestamp() - channelTimestamp ) <= timeout );

    // Check this twice since it can be reported with a delay?
    if( discover->timed_out() )
    {
        return EXIT_CONNECTION_ERR;
    }

    if( !channel )
    {
        PRINT_ERR( "Timed out while trying to connect to the scheduler.\n" );
        return EXIT_CONNECTION_ERR;
    }

    channel->setBulkTransfer();

    if( !channel->send_msg( MonLoginMsg() ) )
    {
        PRINT_ERR( "MsgChannel::send_msg(): Scheduler rejected the MonLoginMsg message.\n" );

        return EXIT_CONNECTION_ERR;
    }

    struct pollfd pollData { channel->fd, POLLIN | POLLPRI, 0 };

    int pollRes = poll( &pollData, 1, timeout );

    if( pollRes < 0 )
    {
        int lastErrno = errno;

        PRINT_ERR( "poll(): (%d) %s\n", lastErrno, strerror( lastErrno ) );

        return EXIT_CONNECTION_ERR;
    }
    else if( pollRes == 0 )
    {
        PRINT_ERR( "poll(): Timed out white awaiting response from the scheduler.\n" );

        return EXIT_CONNECTION_ERR;
    }

    std::vector<std::unique_ptr<NodeInfo>> nodes;
    uint32_t hostIdMax = 0;

    while( !channel->read_a_bit() || channel->has_msg() )
    {
        std::unique_ptr<Msg> msg( channel->get_msg() );

        if( !msg )
        {
            PRINT_ERR( "MsgChannel::get_msg(): No message received from the scheduler.\n" );

            return EXIT_CONNECTION_ERR;
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

            return EXIT_CONNECTION_ERR;
        }
        else
        {
            PRINT_WARN( "Unknown message type: %d\n", msg->type );

            continue;
        }
    }

    if( nodes.empty() ||
        std::all_of( nodes.cbegin(), nodes.cend(), [] ( const std::unique_ptr<NodeInfo>& node ) -> bool
        {
            return ( noOffline && node->isOffline() ) || ( noNoRemote && node->noRemote() );
        } ) )
    {
        PRINT_ERR( "No useful data retrieved.\n" );

        return EXIT_NO_DATA;
    }
    else
    {
        std::uint32_t coreCount = std::accumulate( nodes.cbegin(), nodes.cend(), static_cast<std::uint32_t>( 0 ), [] ( std::uint32_t count, const std::unique_ptr<NodeInfo>& node )
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

        std::uint32_t nodeCount = std::count_if( nodes.cbegin(), nodes.cend(), [] ( const std::unique_ptr<NodeInfo>& node ) { return ( !noOffline || !node->isOffline() ) && ( !noNoRemote || !node->noRemote() ); } );

        if( brief )
        {
            printf( "%u\n", coreCount );
        }
        else
        {
            if( !noTable )
            {
                const std::vector<ColumnHeader> header {
                    { Alignment::Right , false, "Node #"     },
                    { Alignment::Center, true,  "Offline?"   },
                    { Alignment::Center, true,  "No remote?" },
                    { Alignment::Left  , true,  "Name"       },
                    { Alignment::Left  , false, "IP"         },
                    { Alignment::Right , false, "Cores"      },
                    { Alignment::Left  , false, "Platform"   }
                };

                std::vector<std::string> strings;
                strings.reserve( nodeCount * header.size() );

                for( const auto& node : nodes )
                {
                    if( ( !noOffline || !node->isOffline() ) && ( !noNoRemote || !node->noRemote() ) )
                    {
                        strings.emplace_back( std::to_string( node->hostId() ) );
                        strings.emplace_back( node->isOffline() ? ( ascii ? TICK_LO : TICK_HI ) : ( ascii ? NO_TICK_LO : NO_TICK_HI ) );
                        strings.emplace_back( node->noRemote()  ? ( ascii ? TICK_LO : TICK_HI ) : ( ascii ? NO_TICK_LO : NO_TICK_HI ) );
                        strings.push_back( node->name() );
                        strings.push_back( node->ip() );
                        strings.emplace_back( std::to_string( node->maxJobs() ) );
                        strings.push_back( node->platform() );
                    }
                }

                printf( "\n%s\n", renderTable( header, strings, plain, ascii ).c_str() );
            }

            printf( "%u node%s, %u core%s total.\n", nodeCount, nodeCount == 1 ? "" : "s", coreCount, coreCount == 1 ? "" : "s" );
        }

        return EXIT_OK;
    }
}
