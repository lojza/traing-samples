-module(demo7_fsm).

-export([start_link/3, stop/1]).
-export([get_state/1, connect/1, disconnect/1]).
-export([idle/2, connected/2, waiting_retry/2]).
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(loop_data, {
   addr,
   port,
   socket
}).

    
%%----------------------------------------------------------------------
%%  O & M functions
%%----------------------------------------------------------------------

start_link(Name, Addr, Port) ->
    gen_fsm:start_link({local, Name}, ?MODULE, [Addr, Port], []).

stop (ProcRef) ->
    gen_fsm:send_all_state_event(ProcRef, stop).


%%----------------------------------------------------------------------
%%  API functions
%%----------------------------------------------------------------------

get_state(ProcRef) ->
    gen_fsm:sync_send_all_state_event(ProcRef, get_state).

connect(ProcRef) ->
    gen_fsm:send_event(ProcRef, connect).

disconnect(ProcRef) ->
    gen_fsm:send_event(ProcRef, disconnect).

%%----------------------------------------------------------------------
%%  gen_fsm callbacks
%%----------------------------------------------------------------------

init ([Addr, Port]) ->
    LoopData = #loop_data {addr = Addr, port = Port},
    {ok, idle, LoopData}.

handle_event (stop, _StateName, LoopData) ->
    {stop, normal, LoopData}.

handle_sync_event(get_state, _From, StateName, LoopData) ->
    {reply, StateName, StateName, LoopData}.

handle_info({tcp_closed,Socket}, connected, #loop_data{socket = Socket} = LoopData) ->
    gen_fsm:send_event(self(), connect),
    {next_state, idle, LoopData#loop_data{socket = undefined}};
handle_info(Info, StateName, LoopData) -> 
    error_logger:info_report({"unexpected msg:", Info, "loop_data:", LoopData}),
    {next_state, StateName, LoopData}.

terminate(_Reason, _StateName, _LoopData) -> ok.

code_change(_OldVsn, StateName, LoopData, _Extra) -> 
    {ok, StateName, LoopData}.

%%----------------------------------------------------------------------
%%  state handlers
%%----------------------------------------------------------------------

idle (connect, #loop_data{addr = Addr, port = Port} = LoopData) ->
    case gen_tcp:connect(Addr, Port, [], 1000) of
        {ok, Socket} ->
            {next_state, connected, LoopData#loop_data{socket = Socket}};
        {error, _Reason} ->
            {next_state, waiting_retry, LoopData, 2000}
    end;

idle (_Other, LoopData) ->
    {next_state, idle, LoopData}.

connected (disconnect, #loop_data{socket = Socket} = LoopData) ->
    gen_tcp:close(Socket),
    {next_state, idle, LoopData#loop_data{socket = undefined}};
connected (_Other, LoopData) ->
    {next_state, connected, LoopData}.

waiting_retry (Event, LoopData) when (Event == connect) or (Event == timeout) ->
    gen_fsm:send_event(self(), connect),
    {next_state, idle, LoopData};
waiting_retry (_Other, LoopData) ->
    {next_state, waiting_retry, LoopData}.
