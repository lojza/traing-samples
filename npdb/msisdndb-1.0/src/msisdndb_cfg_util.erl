%% =============================================================================
%%  configuration helpers
%% =============================================================================

-module(msisdndb_cfg_util).

-export([db_path/1, input_path/1, input_path/2]).

% database file name (if set application env db_path)
db_path (FileName) ->
  case application:get_env(db_path) of
    {ok, Path} -> filename:join([Path, FileName]);
    undefined -> undefined
  end.

input_path (TypeKey) ->
  input_path (TypeKey, []).

input_path (TypeKey, Opts) ->
  case proplists:is_defined(TypeKey, Opts) of
    true  ->
      P = proplists:get_value(TypeKey, Opts),
      {ok, P};
    false ->
      application:get_env(TypeKey)
  end.
