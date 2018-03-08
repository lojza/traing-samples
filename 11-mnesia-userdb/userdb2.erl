-module(userdb2).

-export([create_tables/0, ensure_loaded/0, insert_user/2, lookup/1, lookup_by_login/1,delete/1]).

-include("userdb2.hrl").

create_tables() ->
    mnesia:create_table(user, [{ram_copies, nodes()}, {type, set},
                               {attributes, record_info(fields, user)}, {index, [login]}]).

ensure_loaded() ->
    ok = mnesia:wait_for_tables([usrdb],1000).

insert_user(Id, Login) ->
    Fun = fun() ->
                  mnesia:write(#user{id = Id, login = Login})
          end,
    mnesia:transaction(Fun).

lookup(Id) ->
    Fun = fun() ->
                  mnesia:read(user, Id)
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.

lookup_by_login(Login) ->
    mnesia:dirty_index_read(user, Login, #user.login).

delete(Id) ->
    Fun = fun() ->
                  mnesia:delete({user, Id})
          end,
    {atomic, Result} = mnesia:transaction(Fun),
    Result.
