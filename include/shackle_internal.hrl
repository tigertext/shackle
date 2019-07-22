-include("shackle.hrl").
-include("shackle_defaults.hrl").

%% macros
-define(APP, shackle).
-define(LOOKUP(Key, List), ?LOOKUP(Key, List, undefined)).
-define(LOOKUP(Key, List, Default), shackle_utils:lookup(Key, List, Default)).
-define(MSG_CONNECT, connect).
-define(SERVER, shackle_server).
-define(SUPERVISOR, shackle_sup).
-define(SERVER_UTILS, shackle_server_utils).
-define(WARN(PoolName, Format, Data), shackle_utils:warning_msg(PoolName, Format, Data)).

%% ETS tables
-define(ETS_TABLE_BACKLOG, shackle_backlog).
-define(ETS_TABLE_POOL_INDEX, shackle_pool_index).
-define(ETS_TABLE_POOL_BAD_WORKERS, shackle_pool_bad_workers).
-define(ETS_TABLE_QUEUE, shackle_queue).
-define(ETS_TABLE_POOL_BAD_WORKER_NUMBERS, shackle_pool_bad_worker_number).

%% compatibility
-ifdef(OTP_RELEASE). %% this implies 21 or higher
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-else.
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-define(GET_STACK(_), erlang:get_stacktrace()).
-endif.
