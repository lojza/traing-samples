%% ==============================================
%%  User database ETS functions
%% ==============================================
-module(userdb_tab).

-export([create_tables/1,
         close_tables/0,
         restore_from_disk/0,
         insert_user/1,
         update_user/1,
         lookup_id/1,
         lookup_login/1,
         delete_user/1,
         delete_disabled/0
        ]).

-include("userdb.hrl").

-define(USERDB_RAM,  userdb_ram).
-define(USERDB_DISK, userdb_dets).
-define(USERDB_IDX,  userdb_index).


create_tables(DiskFile) ->
    ets:new(?USERDB_RAM, [named_table, set, {keypos, #user.id}]),
    ets:new(?USERDB_IDX, [named_table, set, {keypos, 1}]),
    dets:open_file(?USERDB_DISK, [{file, DiskFile}, {keypos, #user.id}]).

close_tables() ->
    ets:delete(?USERDB_RAM),
    ets:delete(?USERDB_IDX),
    dets:close(?USERDB_DISK).

restore_from_disk() ->
    dets:traverse(?USERDB_DISK,
      fun(#user{id = Id, login = Login} = User) ->
          ets:insert(?USERDB_RAM, User),
          ets:insert(?USERDB_IDX, {Login, Id}),
          continue
      end).

insert_user(#user{id = Id, login = Login} = User) ->
    case ets:member(?USERDB_RAM, Id) of
        false ->
            case ets:member(?USERDB_IDX, Login) of
                false -> update_user(User);
                true -> {error, login_exists}
            end;
        true -> {error, id_exists}
    end.

update_user(#user{id = Id, login = Login} = User) ->

    Check = case ets:lookup(?USERDB_IDX, Login) of
                [{Login, Id}] -> ok;
                [{Login, OtherId}] -> {error, {login_used, OtherId}};
                [] -> ok
            end,
    case Check of
        ok ->
            ets:insert(?USERDB_RAM, User),
            ets:insert(?USERDB_IDX, {Login, Id}),
            dets:insert(?USERDB_DISK, User),
            ok;
        Error -> Error
    end.

delete_user(Id) ->
    case lookup_id(Id) of
        {ok, #user{login = Login}} ->
            ets:delete(?USERDB_RAM, Id),
            ets:delete(?USERDB_IDX, Login),
            dets:delete(?USERDB_DISK, Id);
        {error, Reason} -> {error, Reason}
    end.

lookup_id (Id) ->
    case ets:lookup(?USERDB_RAM, Id) of
        [User] -> {ok, User};
        [] -> {error, instance}
    end.

lookup_login (Login) ->
    case ets:lookup(?USERDB_IDX, Login) of
        [{_, Id}] -> lookup_id (Id);
        [] -> {error, instance}
    end.

delete_disabled() ->
    ets:safe_fixtable(?USERDB_RAM, true),
    N = delete_disabled_loop(ets:first(?USERDB_RAM), 0),
    ets:safe_fixtable(?USERDB_RAM, false),
    N.

delete_disabled_loop('$end_of_table', N) -> N;
delete_disabled_loop(Id, N) ->
    case ets:lookup(?USERDB_RAM, Id) of
             [#user{enabled = false}] -> delete_user(Id), NewN = N + 1;
              _ -> NewN = N
    end,
    delete_disabled_loop(ets:next(?USERDB_RAM, Id), NewN).
