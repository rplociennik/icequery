/*
    Copyright (C) 2014 Robert Płóciennik

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/

#include <cstdio>
#include <memory>       // unique_ptr
#include <algorithm>
#include <locale>       // isblank()
#include <vector>

#include <ctime>        // clock_gettime()
#include <cstring>      // strerror()
#include <cstdint>      // uint32_t
#include <cctype>       // tolower
#include <cstdlib>      // abort()

#include <poll.h>
#include <getopt.h>
#include <unistd.h>     // isatty()

#include <icecc/comm.h>
#include <icecc/logging.h>

#define U_HIDE_DRAFT_API
#define U_HIDE_DEPRECATED_API
#define U_HIDE_OBSOLETE_API
#define U_HIDE_SYSTEM_API
#define U_HIDE_INTERNAL_API

#include <unicode/unistr.h>     // icu::UnicodeString
#include <unicode/translit.h>   // icu::Transliterator
#include <unicode/errorcode.h>  // icu::ErrorCode
#include <unicode/ucnv.h>       // icu::ucnv_getDefaultName
#include <unicode/ustdio.h>     // icu::u_printf()

// Utility macros

#define PRINT_STREAM stderr

#define PRINT_BASE( shouldPrint, colorSeq, format, args... ) \
    do \
    { \
        if( !veryQuiet && shouldPrint ) \
        { \
            if( useColor ) \
            { \
                fprintf( PRINT_STREAM, "\e[" colorSeq format "\e[0m", ##args ); \
            } \
            else \
            { \
                fprintf( PRINT_STREAM, format, ##args ); \
            } \
        } \
    } \
    while( 0 )

#define PRINT_INFO(  format, args... )  PRINT_BASE( true,  "1;32m", format, ##args )
#define PRINT_WARN(  format, args... )  PRINT_BASE( true,  "1;33m", format, ##args )
#define PRINT_ERR(   format, args... )  PRINT_BASE( true,  "31m",   format, ##args )
#define PRINT_DEBUG( format, args... )  PRINT_BASE( debug, "1;36m", format, ##args )

#define STR_INTERNAL( x )               #x
#define STR( x )                        STR_INTERNAL( x )

// Version consts

#define VER_NO                      20140705

// Table consts

#define VERT_LINE_7BIT              "|"
#define CROSS_7BIT                  "+"
#define HOR_LINE_7BIT               "-"

#define TICK_7BIT                   "X"
#define NO_TICK_7BIT                ""

#define VERT_LINE_UTF8              "│"
#define CROSS_UTF8                  "┼"
#define HOR_LINE_UTF8               "─"

#define TICK_UTF8                   "√"
#define NO_TICK_UTF8                ""

// Other consts

#define TIMEOUT_DEFAULT             2000
#define RTIMEOUT_DEFAULT            2000

// Exit codes

#define EXIT_OK                     0

#define EXIT_INVALID_ARGS           1
#define EXIT_CONNECTION_ERR         2
#define EXIT_NO_DATA                3
#define EXIT_LIBRARY_ERR            4

// Global variables

std::string netName;
int timeout = TIMEOUT_DEFAULT;
int rtimeout = RTIMEOUT_DEFAULT;
std::string schedAddr;
uint16_t schedPort = 0;

bool quiet = false;
bool veryQuiet = false;
bool brief = false;

std::string customEncoding;
bool plain = false;
bool ascii = false;
bool noTable = false;

bool noOffline = false;
bool noNoRemote = false;

bool debug = false;
bool useColor = false;

// Global consts

const char* VersionStr = \
R"ver(icequery version )ver" STR( VER_NO ) R"ver(
Copyright (C) 2014 Robert Płóciennik
Licensed under GPLv2
)ver";

const char* UsageStr = \
R"usage(usage: %s [options...]

General options:

 -h, --help             : display this info
 -v, --version          : display version info

Connection options:

 -n, --net-name=<NAME>  : net name to use when connecting to the scheduler
 -t, --timeout=<MSECS>  : timeout for establishing connection with the sceduler
                          (default: )usage" STR( TIMEOUT_DEFAULT ) R"usage()
 -r, --rtimeout=<MSECS> : timeout for retrieving a single message from the
                          scheduler (default: )usage" STR( RTIMEOUT_DEFAULT ) R"usage()
     --addr=<ADDRESS>   : scheduler address for avoiding broadcasting and
                          attempting to connect directly
     --port=<PORT>      : scheduler port for direct connection

General output options:

     --color=<WHEN>     : whether to colorize own messages;
                          WHEN can be: 'auto' (default), 'always', or 'never'

 -q, --quiet            : suppress any icecc debug messages sent to stderr
 -Q, --very-quiet       : suppress all error messages entirely
 -b, --brief            : on success return only a single numeric value
                          representing the number of available cores
                          (implies --very-quiet, --no-table)
     --debug            : print debug output during execution

Table options:

     --encoding=<ENC>   : encoding to use when interpreting names
                          (platform default: '%s', must be a valid ICU encoding)
 -P, --plain            : print the table without any borders
 -A, --ascii            : produce only 7-bit ASCII output by displaying table
                          borders as 7-bit characters and performing
                          transliteration on any name encountered
 -T, --no-table         : do not print the table entirely, only a summary on
                          success

 --no-offline  [*]      : do not include offline nodes in the table
 --no-noremote [*]      : do not include 'no remote' nodes in the table

 [*] Selected options affect the display of the table only, as neither offline
     nor 'no remote' nodes are taken into account when calculating totals.

Exit codes:

 0 : No errors occurred

 1 : Command-line error
 2 : Connection error
 3 : No useful data retrieved
 4 : Library error
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

bool checkEncoding( const std::string& encoding )
{
    if( encoding.empty() || std::all_of( encoding.cbegin(), encoding.cend(), std::isblank ) )
    {
        return false;
    }

    icu::ErrorCode errorCode;
    UConverter* converter = ucnv_open( encoding.c_str(), errorCode );

    if( converter )
    {
        ucnv_close( converter );
    }

    if( errorCode.isFailure() )
    {
        PRINT_ERR( "ucnv_open(): %s\n", errorCode.errorName() );
    }

    return !errorCode.isFailure();
}

// Types/Classes

enum class Alignment : char
{
    Left,
    Right,
    Center
};

enum class Encoding : char
{
    UTF8,
    Custom
};

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
        , m_maxJobs( 0 )
        , m_noRemote( false )
        , m_offline( false )
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
    uint32_t m_hostId;
    std::string m_name;
    std::string m_ip;
    uint32_t m_maxJobs;
    bool m_noRemote;
    bool m_offline;
    std::string m_platform;
};

class ColumnHeader
{
public:
    ColumnHeader( Alignment alignment, Encoding encoding, const std::string& name )
        : m_alignment( alignment )
        , m_encoding( encoding )
        , m_name( name )
    {
    }

    ColumnHeader( Alignment alignment, Encoding encoding, const std::string&& name )
        : m_alignment( alignment )
        , m_encoding( encoding )
        , m_name( std::move( name ) )
    {
    }

public:
    Alignment alignment() const
    {
        return m_alignment;
    }

    Encoding encoding() const
    {
        return m_encoding;
    }

    const std::string& name() const
    {
        return m_name;
    }

private:
    Alignment m_alignment;
    Encoding m_encoding;
    std::string m_name;
};

// Other functions

const char* MsgTypeToStr( MsgType msgType )
{
    switch( msgType )
    {
        case MsgType::M_MON_GET_CS:
            return "M_MON_GET_CS";

        case MsgType::M_MON_JOB_BEGIN:
            return "M_MON_JOB_BEGIN";

        case MsgType::M_MON_JOB_DONE:
            return "M_MON_JOB_DONE";

        case MsgType::M_MON_LOCAL_JOB_BEGIN:
            return "M_MON_LOCAL_JOB_BEGIN";

        case MsgType::M_MON_STATS:
            return "M_MON_STATS";

        default:
            return "<Unknown>";
    }
}

icu::UnicodeString renderTable( const std::vector<ColumnHeader>& header, const std::vector<std::string>& strings, bool plain, bool ascii )
{
    auto columnCount = header.size();
    auto rowCount = strings.size() / columnCount;

    std::vector<std::size_t> colMaxLens( columnCount );

    std::unique_ptr<icu::Transliterator> trans;
    std::vector<icu::UnicodeString> data( columnCount * ( rowCount + 1 ) );
    std::vector<std::size_t> lens( columnCount * ( rowCount + 1 ) );

    // Init Transliterator
    if( ascii )
    {
        icu::ErrorCode errorCode;
        trans.reset( icu::Transliterator::createInstance( "Latin-ASCII", UTRANS_FORWARD, errorCode ) );

        if( errorCode.isFailure() )
        {
            PRINT_ERR( "icu::Transliterator::createInstance(): %s\n", errorCode.errorName() );
            std::exit( EXIT_LIBRARY_ERR );
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
            icu::UnicodeString& res = data[columnCount * r + c];

            const std::string& origStr = r == 0 ? header[c].name() : strings[columnCount * (r - 1) + c];
            const Encoding encoding = header[c].encoding();

            if( r == 0 || encoding == Encoding::UTF8 )
            {
                res = std::move( icu::UnicodeString::fromUTF8( origStr ) );
            }
            else
            {
                res = std::move( icu::UnicodeString( origStr.c_str(), customEncoding.c_str() ) );
            }

            if( trans )
            {
                trans->transliterate( res );
            }

            len = static_cast<std::size_t>( res.length() );

            // Add 1 char margin to the first and last column
            if( !plain )
            {
                if( c == 0 )
                {
                    res.insert( 0, ' ' );
                    ++len;
                }
                else if( c == columnCount - 1 )
                {
                    res.append( ' ' );
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
            icu::UnicodeString& str = data[columnCount * r + c];
            const std::size_t& len = lens[columnCount * r + c];

            if( len < maxLen )
            {
                std::size_t lenDiff = maxLen - len;

                switch( align )
                {
                    case Alignment::Left:
                        str.append( icu::UnicodeString::fromUTF8( std::string( lenDiff, ' ' ) ) );
                        break;

                    case Alignment::Right:
                        str.insert( 0, icu::UnicodeString::fromUTF8( std::string( lenDiff, ' ' ) ) );
                        break;

                    case Alignment::Center:
                        str.insert( 0, icu::UnicodeString::fromUTF8( std::string( lenDiff / 2, ' ' ) ) ).append( icu::UnicodeString::fromUTF8( std::string( lenDiff - lenDiff / 2, ' ' ) ) );
                        break;
                }
            }
        }
    }

    // Render the table
    icu::UnicodeString table;

    for( std::size_t r = 0; r < rowCount + 1; ++r )
    {
        for( std::size_t c = 0; c < columnCount; ++c )
        {
            if( c > 0 )
            {
                if( plain )
                {
                   table.append( ' ' );
                }
                else
                {
                    table.append( icu::UnicodeString::fromUTF8( std::string( ascii ? ( " " VERT_LINE_7BIT " " ) : ( " " VERT_LINE_UTF8 " " ) ) ) );
                }
            }

            table.append( data[columnCount * r + c] );
        }

        table.append( '\n' );

        // Draw the horizontal line below the header
        if( r == 0 && !plain )
        {
            for( std::size_t c = 0; c < columnCount; ++c )
            {
                if( c > 0 )
                {
                    table.append( ascii ? ( HOR_LINE_7BIT CROSS_7BIT HOR_LINE_7BIT ) : ( HOR_LINE_UTF8 CROSS_UTF8 HOR_LINE_UTF8 ) );
                }

                for( std::size_t i = 0; i < colMaxLens[c]; ++i )
                {
                    table.append( ascii ? HOR_LINE_7BIT : HOR_LINE_UTF8 );
                }
            }

            table.append( '\n' );
        }
    }

    return std::move( table );
}

// And the entry point...

int main( int argc, char** argv )
{
    // Set the defaults

    int fd = fileno( PRINT_STREAM );

    if( fd == -1 )
    {
        useColor = false;
    }
    else
    {
        useColor = ( isatty( fd ) == 1 );
    }

    customEncoding.assign( ucnv_getDefaultName() );

    // Command-line option parsing

    // Disable built-in error messages
    opterr = 0;

    int currOpt;

    while( true )
    {
        static struct option options[] = {
            { "help",        no_argument,       0, 'h' },
            { "version",     no_argument,       0, 'v' },

            { "net-name",    required_argument, 0, 'n' },
            { "timeout",     required_argument, 0, 't' },
            { "rtimeout",    required_argument, 0, 'r' },
            { "addr",        required_argument, 0,  1  },
            { "port",        required_argument, 0,  2  },

            { "color",       required_argument, 0,  5  },
            { "quiet",       no_argument,       0, 'q' },
            { "very-quiet",  no_argument,       0, 'Q' },
            { "brief",       no_argument,       0, 'b' },
            { "debug",       no_argument,       0,  13 },

            { "encoding",    required_argument, 0,  6  },
            { "plain",       no_argument,       0, 'P' },
            { "ascii",       no_argument,       0, 'A' },
            { "no-table",    no_argument,       0, 'T' },

            { "no-offline",  no_argument,       0,  3  },
            { "no-noremote", no_argument,       0,  4  },

            { 0,             0,                 0,  0  }
        };

        int optRes = getopt_long( argc, argv, ":hvn:t:r:qQbPAT", options, &currOpt );

        if( optRes == -1 )
        {
            break;
        }

        switch( optRes )
        {
            case 'h':
                fprintf( stderr, UsageStr, argv[0], customEncoding.c_str() );
                return EXIT_INVALID_ARGS;

            case 'v': // version
                printf( VersionStr );
                return EXIT_OK;

            case '?':
                PRINT_ERR( "Unknown/ambiguous option '%s'. Try '--help'.\n", argv[optind - 1] );
                return EXIT_INVALID_ARGS;

            case ':':
                PRINT_ERR( "Missing argument for '%s'.\n", options[currOpt].name );
                return EXIT_INVALID_ARGS;

            case 'n':
                netName.assign( optarg );
                break;

            case 't':
                if( sscanf( optarg, "%u", &timeout ) != 1 )
                {
                    PRINT_ERR( "Invalid argument for '%s'.\n", options[currOpt].name );
                    return EXIT_INVALID_ARGS;
                }
                break;

            case 'r':
                if( sscanf( optarg, "%u", &rtimeout ) != 1 )
                {
                    PRINT_ERR( "Invalid argument for '%s'.\n", options[currOpt].name );
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

            case 6: // encoding
                customEncoding.assign( optarg );

                if( !checkEncoding( customEncoding ) )
                {
                    PRINT_ERR( "Invalid ICU encoding '%s'.\n", optarg );
                    return EXIT_INVALID_ARGS;
                }

                break;

            case 'P':
                plain = true;
                break;

            case 'A':
                ascii = true;
                break;

            case 'T':
                noTable = true;
                break;

            case 1: // addr
                schedAddr.assign( optarg );
                break;

            case 2: // port
                if( sscanf( optarg, "%hu", &schedPort ) != 1 )
                {
                    PRINT_ERR( "Invalid argument for '%s'.\n", options[currOpt].name );
                    return EXIT_INVALID_ARGS;
                }
                break;

            case 3: // no-offline
                noOffline = true;
                break;

            case 4: // no-noremote
                noNoRemote = true;
                break;

            case 5: // color
                if( strcmp( optarg, "always" ) == 0 )
                {
                    useColor = true;
                }
                else if( strcmp( optarg, "never" ) == 0 )
                {
                    useColor = false;
                }
                else if( strcmp( optarg, "auto" ) != 0 )
                {
                    PRINT_ERR( "Invalid argument for '%s'.\n", options[currOpt].name );
                    return EXIT_INVALID_ARGS;
                }
                break;

            case 13: // debug
                debug = true;
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

    PRINT_INFO( "Attempting to connect to the scheduler...\n" );

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

    PRINT_INFO( "Retrieving messages...\n" );

    struct pollfd pollData { channel->fd, POLLIN | POLLPRI, 0 };

    std::vector<std::unique_ptr<NodeInfo>> nodes;
    uint32_t hostIdMax = 0;

    int pollRes;
    bool wasPollUseful;

    do
    {
        pollRes = poll( &pollData, 1, rtimeout );
        wasPollUseful = false;
        static uint32_t msgNo = 0;

        if( pollRes < 0 )
        {
            int lastErrno = errno;

            PRINT_ERR( "poll(): (%d) %s\n", lastErrno, strerror( lastErrno ) );

            return EXIT_CONNECTION_ERR;
        }
        else if( pollRes > 0 )
        {
            while( !channel->read_a_bit() || channel->has_msg() )
            {
                std::unique_ptr<Msg> msg( channel->get_msg() );

                if( !msg )
                {
                    PRINT_ERR( "MsgChannel::get_msg(): No messages received from the scheduler.\n" );

                    return EXIT_CONNECTION_ERR;
                }

                bool isMsgUseful = false;
                ++msgNo;

                if( msg->type == M_MON_STATS )
                {
                    const MonStatsMsg* statsMsg = dynamic_cast<MonStatsMsg*>(msg.get());

                    PRINT_DEBUG( "\nMessage %u:\n-\n%s-\n", msgNo, statsMsg->statmsg.c_str() );

                    auto nodeInfo = std::move( NodeInfo::create( statsMsg->hostid, statsMsg->statmsg ) );

                    if( nodeInfo && nodeInfo->hostId() > hostIdMax)
                    {
                        // Keep track of highest hostId in case the scheduler sends multiple copies of the same hostInfos
                        hostIdMax = nodeInfo->hostId();
                        nodes.push_back( std::move( nodeInfo ) );
                        wasPollUseful |= ( isMsgUseful = true );
                    }
                }
                else if( msg->type == M_END )
                {
                    PRINT_ERR( "Received M_END (%d). Scheduler has quit.\n", M_END );

                    return EXIT_CONNECTION_ERR;
                }
                else
                {
                    PRINT_DEBUG( "Message %u of type %s (%d) ignored\n", msgNo, MsgTypeToStr( msg->type ), msg->type );
                }

                if( !isMsgUseful )
                {
                    PRINT_DEBUG( "Message %u considered useless\n", msgNo );
                }
            }
        }
    } while( pollRes != 0 && wasPollUseful );

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
                    { Alignment::Right , Encoding::UTF8     , "Node #"     },
                    { Alignment::Center, Encoding::UTF8     , "Offline?"   },
                    { Alignment::Center, Encoding::UTF8     , "No remote?" },
                    { Alignment::Left  , Encoding::Custom   , "Name"       },
                    { Alignment::Left  , Encoding::UTF8     , "IP"         },
                    { Alignment::Right , Encoding::UTF8     , "Cores"      },
                    { Alignment::Left  , Encoding::UTF8     , "Platform"   }
                };

                std::vector<std::string> strings;
                strings.reserve( nodeCount * header.size() );

                for( const auto& node : nodes )
                {
                    if( ( !noOffline || !node->isOffline() ) && ( !noNoRemote || !node->noRemote() ) )
                    {
                        strings.emplace_back( std::to_string( node->hostId() ) );
                        strings.emplace_back( node->isOffline() ? ( ascii ? TICK_7BIT : TICK_UTF8 ) : ( ascii ? NO_TICK_7BIT : NO_TICK_UTF8 ) );
                        strings.emplace_back( node->noRemote()  ? ( ascii ? TICK_7BIT : TICK_UTF8 ) : ( ascii ? NO_TICK_7BIT : NO_TICK_UTF8 ) );
                        strings.push_back( node->name() );
                        strings.push_back( node->ip() );
                        strings.emplace_back( std::to_string( node->maxJobs() ) );
                        strings.push_back( node->platform() );
                    }
                }

                u_fputs( renderTable( header, strings, plain, ascii ).insert( 0, '\n' ).getTerminatedBuffer(), u_get_stdout() );
            }

            printf( "%u node%s, %u core%s total.\n", nodeCount, nodeCount == 1 ? "" : "s", coreCount, coreCount == 1 ? "" : "s" );
        }

        return EXIT_OK;
    }
}
