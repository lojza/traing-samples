-module(demo8_handler).
-behaviour(gen_event).

-export([start/0, stop/0, get_stat/0]).
-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    ok_count,
    err_dict
}).

-define(EVENT_MANAGER, demo8_event).

%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start() ->
    gen_event:add_handler(?EVENT_MANAGER, ?MODULE, []).

stop() ->
    demo8_event:delete_handler(?EVENT_MANAGER, ?MODULE, []).


%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

get_stat() ->
    gen_event:call(?EVENT_MANAGER, ?MODULE, get_stat).

reset() ->
    gen_event:call(?EVENT_MANAGER, ?MODULE, reset).

%% ----------------------------------------------------------------------------
%%  gen_event callbacks
%% ----------------------------------------------------------------------------

init([]) ->
    {ok, #state{ok_count = 0, err_dict = orddict:new()}}.

handle_event(ok, #state{ok_count = OkCount} = State) ->
    % increment ok counter
    {ok,State#state{ok_count = OkCount + 1}};

handle_event({error, Reason}, #state{err_dict = Orddict1} = State) ->
    % increment error counter
    Orddict2 = orddict:update_counter(Reason, 1, Orddict1),
    {ok,State#state{err_dict = Orddict2}};

handle_event(_Other, State) ->
    % silently ignore
    {ok,State}.

handle_call(get_stat, #state{ok_count = OkCount, err_dict = Orddict} = State) ->
    {ok,{OkCount, orddict:to_list(Orddict)},State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
