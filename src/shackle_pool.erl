-module(shackle_pool).
-include("shackle_internal.hrl").
-behavior(gen_server).

-ignore_xref([
    {shackle_pool_foil, lookup, 1}
]).
-type(state():: map()).

%% public
%% gen_server callbacks.
-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3, terminate/2]).
%% APIs
-export([
    start/3,
    start/4,
    stop/1
]).
-export([
    worker_down/2,
    worker_up/2
]).
%% internal
-export([
    server/1,
    terminate/0
]).
%% public
-spec start(pool_name(), client(), client_options()) ->
    ok | {error, shackle_not_started | pool_already_started}.

start(Name, Client, ClientOptions) ->
    start(Name, Client, ClientOptions, []).

-spec start(pool_name(), client(), client_options(), pool_options()) ->
    ok | {error, shackle_not_started | pool_already_started}.

start(Name, Client, ClientOptions, Options) ->
    case options(Name) of
        {ok, _OptionsRec} ->
            {error, pool_already_started};
        {error, shackle_not_started} ->
            {error, shackle_not_started};
        {error, pool_not_started} ->
            gen_server:call(?MODULE, {start_pool, Name, Client, ClientOptions, Options}),
            ok
    end.

-spec stop(pool_name()) ->
    ok | {error, shackle_not_started | pool_not_started}.

stop(Name) ->
    case options(Name) of
        {ok, #pool_options{} = OptionsRec} ->
            gen_server:call(?MODULE, {stop_pool, Name, OptionsRec}),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init(any()) -> {ok, state()}.
init(_) ->
    init_ets(),
    foil:new(?MODULE),
    foil:load(?MODULE),
    process_flag(trap_exit, true),
    {ok, maps:new()}.

-spec handle_call(any(), pid(), state()) -> {reply, any(), state()}.
handle_call({stop_pool, Name, #pool_options{pool_size = PoolSize} = OptionsRec}, _From, WorkersMap) ->
    Workers = server_names(Name, PoolSize),
    WorkersMap2 =
        lists:foldl(fun(Worker, Acc) ->
            case whereis(Worker) of
                undefined ->
                    Acc;
                Pid ->
                    case maps:is_key(Pid, Acc) of
                        true ->
                            erlang:unlink(Pid),
                            exit(Pid, kill),
                            maps:remove(Pid, Acc);
                        _ ->
                            exit(Pid, kill),
                            Acc
                    end
            end
                    end, WorkersMap, Workers),
    
    cleanup_pool(Name, OptionsRec),
    {reply, ok, WorkersMap2};

handle_call({start_pool, Name, Client, ClientOptions, Options}, _From, WorkersMap) ->
    OptionsRec = options_rec(Client, Options),
    setup_pool(Name, OptionsRec),
    NewMap =
        lists:foldl(fun({Pid, Parmas}, Acc) -> maps:put(Pid, Parmas, Acc) end, WorkersMap,
            start_workers(Name, Client, ClientOptions, OptionsRec)),
    {reply, ok, NewMap}.

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({'EXIT', Pid, _Reason}, Workers) ->
    ct:pal("exit ~p ", [{Pid, _Reason}]),
    shackle_utils:warning_msg(undefined, "worker down pid ~p info ~p", [Pid, _Reason]),
    Workers2 =
        case maps:is_key(Pid, Workers) of
            true ->
                Param = #{server_mod := ServerMod, serveer_name := ServerName,
                    pool_name := Pool_Name, client := Client, client_options := ClientOptions, index := Index} = maps:get(Pid, Workers),
                erlang:unlink(Pid),
                shackle_utils:warning_msg(Pool_Name, "worker down pid ~p info ~p", [Pid, Param]),
                worker_down(Pool_Name, Index),
                {ok, Pid2} = ServerMod:start_link(ServerName, Pool_Name, Client, ClientOptions, Index),
                Workers1 = maps:remove(Pid, Workers),
                shackle_utils:warning_msg(Pool_Name, "worker restarted ~p failed workers ~p failed worker number ~p", [Pid, ets:tab2list(?ETS_TABLE_POOL_BAD_WORKERS), ets:tab2list(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS)]),
                maps:put(Pid2, Param, Workers1);
            false ->
                shackle_utils:warning_msg(undefined, "Woker down, cannot be handled, cannot find pool info! pid ~p ", [Pid]),
                ok
        end,
    {noreply, Workers2}.

-spec terminate(any(), any()) -> ok.
terminate(_Reason, _State) ->
    ok.
-spec code_change(any(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec server(pool_name()) ->
    {ok, client(), atom()} | {error, atom()}.

server(Name) ->
    case options(Name) of
        {ok, #pool_options{
            backlog_size = BacklogSize,
            client = Client,
            pool_size = PoolSize,
            pool_strategy = PoolStrategy
        }} ->
            
            case server_index(Name, PoolSize, PoolStrategy) of
                {error, Reason} ->
                    {error, Reason};
                ServerIndex ->
                    Key = {Name, ServerIndex},
                    {ok, Server} = shackle_pool_foil:lookup(Key),
                    case shackle_backlog:check(Server, BacklogSize) of
                        true ->
                            {ok, Client, Server};
                        false ->
                            {error, backlog_full}
                    end
            end;
        {error, Reson} ->
            {error, Reson}
    end.

-spec terminate() ->
    ok.

terminate() ->
    foil:delete(?MODULE).

-spec worker_down(pool_name(), pos_integer()) -> no_return().
worker_down(PoolName, Index) ->
    case ets:lookup(?ETS_TABLE_POOL_BAD_WORKERS, {PoolName, Index}) of
        [_Obj] -> ok;
        _ ->
            ets:insert(?ETS_TABLE_POOL_BAD_WORKERS, {{PoolName, Index}, true}),
            worker_down_cb(ets:update_counter(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS, PoolName, 1), PoolName, options(PoolName))
    end.

-spec worker_up(pool_name(), pos_integer()) -> no_return().
worker_up(PoolName, Index) ->
    ets:delete(?ETS_TABLE_POOL_BAD_WORKERS, {PoolName, Index}),
    worker_up_cb(ets:update_counter(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS, PoolName, -1), PoolName, options(PoolName)).

%% private
cleanup_pool(Name, OptionsRec) ->
    cleanup_ets(Name, OptionsRec),
    cleanup_foil(Name, OptionsRec).

cleanup_ets(Name, #pool_options{pool_strategy = round_robin}) ->
    ets:delete(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS, Name),
    ets:delete(?ETS_TABLE_POOL_INDEX, {Name, round_robin});
cleanup_ets(Name, _OptionsRec) ->
    ets:delete(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS, Name),
    ok.

cleanup_foil(Name, #pool_options{pool_size = PoolSize}) ->
    foil:delete(?MODULE, Name),
    [foil:delete(?MODULE, {Name, N}) || N <- lists:seq(1, PoolSize)],
    foil:load(?MODULE).

options(Name) ->
    try shackle_pool_foil:lookup(Name) of
        {ok, Options} ->
            {ok, Options};
        {error, key_not_found} ->
            {error, pool_not_started}
    catch
        error:undef ->
            {error, shackle_not_started}
    end.

options_rec(Client, Options) ->
    BacklogSize = ?LOOKUP(backlog_size, Options, ?DEFAULT_BACKLOG_SIZE),
    PoolSize = ?LOOKUP(pool_size, Options, ?DEFAULT_POOL_SIZE),
    PoolStrategy = ?LOOKUP(pool_strategy, Options, ?DEFAULT_POOL_STRATEGY),
    PoolFailureThresholdPercentage = ?LOOKUP(pool_failure_threshold_percentage, Options, ?DEFAULT_POOL_FAILURE_THRESHOLD_PERCENTAGE),
    PoolRecoverThresholdPercentage = ?LOOKUP(pool_recover_threshold_percentage, Options, ?DEFAULT_POOL_RECOVER_THRESHOLD_PERCENTAGE),
    FailureCbMod = ?LOOKUP(pool_failure_callback_module, Options),
    RecoverCbMod = ?LOOKUP(pool_recover_callback_module, Options),
    #pool_options{
        backlog_size = BacklogSize,
        client = Client,
        pool_size = PoolSize,
        pool_strategy = PoolStrategy,
        pool_failure_threshold_percentage = PoolFailureThresholdPercentage,
        pool_recover_threshold_percentage = PoolRecoverThresholdPercentage,
        pool_failure_callback_mod = FailureCbMod,
        pool_recover_callback_mod = RecoverCbMod
    }.

server_index(Name, PoolSize, Stratege) ->
    server_index(Name, PoolSize, Stratege, 1).
server_index(Name, PoolSize, _Stratege, N) when N > (0.5 * PoolSize) ->
    %% too many retires and cannot find a live worker. something is wrong with this worker pool
    %% consider disabling it.
    shackle_utils:warning_msg(Name, "Cannot find a live worker after many retries, consider to disable this pool, tried ~p times. ", [N]),
    {error, no_worker};

server_index(Name, PoolSize, random, N) ->
    ServerId = shackle_utils:random(PoolSize),
    check_server_id(Name, PoolSize, ServerId, random, N + 1);
server_index(Name, PoolSize, round_robin, N) ->
    UpdateOps = [{2, 1, PoolSize, 1}],
    Key = {Name, round_robin},
    [ServerId] = ets:update_counter(?ETS_TABLE_POOL_INDEX, Key, UpdateOps),
    check_server_id(Name, PoolSize, ServerId, round_robin, N + 1).

check_server_id(Name, PoolSize, ServerId, Stretegy, N) ->
    case is_worker_disabled(Name, ServerId) of
        true ->
            shackle_utils:warning_msg(Name, "got a dead worker id ~p, try again", [ServerId]),
            server_index(Name, PoolSize, Stretegy, N);
        _ ->
            ServerId
    end.

setup_pool(Name, OptionsRec) ->
    setup_ets(Name, OptionsRec),
    setup_foil(Name, OptionsRec).

setup_ets(Name, #pool_options{pool_strategy = round_robin, pool_size = Size}) ->
    ets:insert_new(?ETS_TABLE_POOL_INDEX, {{Name, round_robin}, 1}),
    ets:insert_new(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS, {Name, Size});
setup_ets(Name, #pool_options{pool_size = Size}) ->
    ets:insert_new(?ETS_TABLE_POOL_BAD_WORKER_NUMBERS, {Name, Size}).

setup_foil(Name, #pool_options{pool_size = PoolSize} = OptionsRec) ->
    foil:insert(?MODULE, Name, OptionsRec),
    [foil:insert(?MODULE, {Name, N}, server_name(Name, N)) ||
        N <- lists:seq(1, PoolSize)],
    foil:load(?MODULE).

server_name(Name, Index) ->
    list_to_atom(atom_to_list(Name) ++ "_" ++ integer_to_list(Index)).

server_names(Name, PoolSize) ->
    [server_name(Name, N) || N <- lists:seq(1, PoolSize)].

server_mod(shackle_ssl) ->
    shackle_ssl_server;
server_mod(shackle_tcp) ->
    shackle_tcp_server;
server_mod(shackle_udp) ->
    shackle_udp_server.


start_workers(Name, Client, ClientOptions, #pool_options{pool_size = PoolSize}) ->
    Protocol = ?LOOKUP(protocol, ClientOptions, ?DEFAULT_PROTOCOL),
    ServerMod = server_mod(Protocol),
    [
        begin
            ServerName = server_name(Name, N),
            {ok, Pid} = ServerMod:start_link(ServerName, Name, Client, ClientOptions, N),
            {Pid, #{server_mod => ServerMod, serveer_name => ServerName,
                pool_name => Name, client=> Client, client_options => ClientOptions, index => N}}
        end || N <- lists:seq(1, PoolSize)
    ].

is_worker_disabled(PoolName, Index) ->
    ets:lookup(?ETS_TABLE_POOL_BAD_WORKERS, {PoolName, Index}) /= [].

init_ets() ->
    [
        ets:new(Tab, [
            named_table,
            public,
            {write_concurrency, true}
        ])
        || Tab <- [?ETS_TABLE_POOL_INDEX, ?ETS_TABLE_POOL_BAD_WORKERS, ?ETS_TABLE_POOL_BAD_WORKER_NUMBERS]
    ].

worker_down_cb(NumberOfFailedWorkers, PoolName,
    {ok, #pool_options{pool_size = PoolSize,
        pool_failure_threshold_percentage = DisablePercentage,
        pool_failure_callback_mod = Mod}}) when (NumberOfFailedWorkers / PoolSize) >= DisablePercentage,
    Mod /= undefined ->
    case erlang:function_exported(Mod, node_down, 2) of
        true ->
            Mod:node_down(PoolName, NumberOfFailedWorkers);
        false ->
            ok
    end;
worker_down_cb(_, _, _) ->
    ok.

worker_up_cb(NumberOfFailedWorkers, PoolName,
    {ok, #pool_options{pool_size = PoolSize,
        pool_recover_threshold_percentage = EnablePercentage,
        pool_recover_callback_mod = Mod}}) when (NumberOfFailedWorkers / PoolSize) =< EnablePercentage,
    Mod /= undefined ->
    case erlang:function_exported(Mod, node_up, 2) of
        true ->
            Mod:node_up(PoolName, NumberOfFailedWorkers);
        false ->
            ok
    end;
worker_up_cb(_, _, _) ->
    ok.

