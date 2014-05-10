#include <cstdio>
#include <memory>
#include <thread>
#include <chrono>

#include <poll.h>

#include <icecc/comm.h>

std::unique_ptr<MsgChannel> channel;    

int threadStatus = 0;

void pollingThreadEntry()
{
    const int maxPollRetries = 50;
    int pollRetries = 0;
    const int pollTimeout = 3000;
    struct pollfd pollData { channel->fd, POLLIN | POLLRDNORM | POLLRDBAND | POLLPRI | POLLRDHUP, 0 };

    while( true )
    {
        int res = poll( &pollData, 1, pollTimeout );

        if( res < 0 )
        {
            fprintf( stderr, "Error ocurred during poll().\n" );

            threadStatus = 1;
            return;
        }
        else if( res == 0 )
        {
            fprintf( stderr, "No data read during poll().\n" );

            if( pollRetries < maxPollRetries )
            {
                fprintf( stderr, "Retry no. %d...\n", ++pollRetries );

                continue;
            }
            else
            {
                threadStatus = 1;
                return;
            }
        }

        while( !channel->read_a_bit() || channel->has_msg() )
        {
            std::unique_ptr<Msg> msg( channel->get_msg() );

            if( !msg )
            {
                fprintf( stderr, "No message received, connection might be broken.\n" );

                threadStatus = 1;
                return;
            }

            switch( msg->type )
            {
                case M_MON_STATS:
                    
                    printf( "%s\n", dynamic_cast<MonStatsMsg*>(msg.get())->statmsg.c_str() );
                    continue;

                case M_END:

                    fprintf( stderr, "Scheduler has quit.\n" );
                    threadStatus = 1;
                    return;

                default:
                    continue;
            }
        }
    }
}

int main( void )
{
    const char* netName = "ICECREAM";
    const int timeout = 2000000;
    const char* schedIp = "";
    const int port = 0;

    std::unique_ptr<DiscoverSched> discover( new DiscoverSched( netName, timeout, schedIp, port ) );

    int retries = 0;
    const int maxRetries = 50;

    do
    {
        channel.reset( discover->try_get_scheduler() );

        if( !channel && retries < maxRetries )
        {
            fprintf( stderr, "Retry no. %d...\n", ++retries );
        }
    } while( !channel && retries < maxRetries );

    if( !channel )
    {
        if( discover->timed_out() )
        {
            fprintf( stderr, "Timed out.\n" );
        }

        fprintf( stderr, "Connection attempt failed.\n" );

        return 1;
    }

    channel->setBulkTransfer();
    discover.reset( nullptr );

    // std::thread pollingThread( pollingThreadEntry );

    std::this_thread::sleep_for( std::chrono::milliseconds( 2000 ) );

    if( !channel->send_msg( MonLoginMsg() ) )
    {
        fprintf( stderr, "Scheduler rejected login message.\n" );

        return 1;
    }

    // pollingThread.join();

    pollingThreadEntry();

    return 0;
}
