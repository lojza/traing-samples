%% ============================================================================
%%  user database server
%% ============================================================================
-module(userdb).
-behaviour(gen_server).

-export([ start_link/0, start_link/1, stop/0]).
-export([ create_user/2, create_user/3,
          delete_user/1,
          enable/1, disable/1,delete_disabled/0,
          set_feature/3, lookup_feature/2,
          lookup_by_login/1, lookup/1
        ]).

-export([init/1, terminate/2, handle_call/3, handle_cast/2,handle_info/2, code_change/3]).

-include("userdb.hrl").

-record(state, {
          file
}).

%% ----------------------------------------------------------------------------
%%  O & M functions
%% ----------------------------------------------------------------------------

start_link() ->
    {ok, DiskFile} = application:get_env(data_file),
    start_link(DiskFile).

start_link(DiskFile) ->
    gen_server:start_link ({local, ?MODULE}, ?MODULE, [DiskFile], []).

stop() ->
    gen_server:cast(?MODULE, stop).

%% ----------------------------------------------------------------------------
%%  API functions
%% ----------------------------------------------------------------------------

create_user(Id, Login) -> create_user(Id, Login, []).

create_user(Id, Login, Features) ->
    gen_server:call(?MODULE, {create_user, Id, Login, Features}).

delete_user(Id) ->
    gen_server:call(?MODULE, {delete_user, Id}).

delete_disabled() ->
    gen_server:call(?MODULE, delete_disabled).

enable(Id) ->
    gen_server:call(?MODULE, {enable, Id, true}).

disable(Id) ->
    gen_server:call(?MODULE, {enable, Id, false}).

set_feature(Id, Key, Value) ->
    gen_server:call(?MODULE, {set_feature, Id, Key, Value}).

lookup(Id) ->
    case userdb_tab:lookup_id(Id) of
        {ok, #user{login = Login}} -> {ok, Login};
        {error, Reason} -> {error, Reason}
    end.

lookup_by_login(Login) ->
    case userdb_tab:lookup_login(Login) of
        {ok, #user{id = Id}} -> {ok, Id};
        {error, Reason} -> {error, Reason}
    end.

lookup_feature(Id, Key) ->
    case userdb_tab:lookup_id(Id) of
        {ok, #user{features = Features}} ->
            case orddict:find(Key, Features) of
                {ok, Value} -> {ok, Value};
                error -> {error, not_set}
            end;
        {error, Reason} -> {error, Reason}
    end.

%% ----------------------------------------------------------------------------
%%  gen_server callback functions
%% ----------------------------------------------------------------------------

init ([DiskFile]) ->
    process_flag(trap_exit, true),
    userdb_tab:create_tables(DiskFile),
    userdb_tab:restore_from_disk(),
    State = #state{file = DiskFile},
    {ok, State}.

handle_call ({create_user, Id, Login, Features}, _From, State) ->
    User = #user{id = Id, login = Login, features = orddict:from_list(Features)},
    Reply = userdb_tab:insert_user(User),
    {reply, Reply, State};

handle_call ({delete_user, Id}, _From, State) ->
    Reply = userdb_tab:delete_user(Id),
    {reply, Reply, State};

handle_call (delete_disabled, _From, State) ->
    Reply = userdb_tab:delete_disabled(),
    {reply, Reply, State};

handle_call ({enable, Id, Flag}, _From, State) ->
    case userdb_tab:lookup_id(Id) of
        {ok, User} ->
            NewUser = User#user{enabled = Flag},
            Reply = userdb_tab:update_user(NewUser),
            {reply, Reply, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call ({set_feature, Id, Key, Value}, _From, State) ->
    case userdb_tab:lookup_id(Id) of
        {ok, #user{features = Features} = User} ->
            NewFeatures = orddict:store(Key, Value, Features),
            NewUser = User#user{features = NewFeatures},
            Reply = userdb_tab:update_user(NewUser),
            {reply, Reply, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

handle_cast (stop, State) ->
    {stop, normal, State}.

handle_info (_Msg, State) ->
    io:format("~p: unexpected message ~p~n", [?MODULE, _Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate (_Reason, _State) ->
    userdb_tab:close_tables().
