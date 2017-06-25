%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_lmdb_store).
-include("vmq_server.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1,
         msg_store_write/2,
         msg_store_read/2,
         msg_store_delete/2,
         msg_store_find/1,
         get_ref/1,
         refcount/1]).

-export([msg_store_init_queue_collector/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type ref() :: {elmdb:env(), elmdb:dbi()}.

-record(state, {ref :: ref(),
                data_root :: string(),
                refs = ets:new(?MODULE, [])
               }).

%%%===================================================================
%%% API
%%%===================================================================
start_link({Id,Env}) ->
    gen_server:start_link(?MODULE, [{Id, Env}], []).

msg_store_write(SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
    call(MsgRef, {write, SubscriberId, Msg}).

msg_store_delete(SubscriberId, MsgRef) ->
    call(MsgRef, {delete, SubscriberId, MsgRef}).

msg_store_read(SubscriberId, MsgRef) ->
    call(MsgRef, {read, SubscriberId, MsgRef}).

msg_store_find(SubscriberId) ->
    Ref = make_ref(),
    {Pid, MRef} = spawn_monitor(?MODULE, msg_store_init_queue_collector,
                                [self(), SubscriberId, Ref]),
    receive
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason};
        {Pid, Ref, Result} ->
            demonitor(MRef, [flush]),
            {_, MsgRefs} = lists:unzip(Result),
            {ok, MsgRefs}
    end.

msg_store_init_queue_collector(ParentPid, SubscriberId, Ref) ->
    Pids = vmq_lmdb_store_sup:get_bucket_pids(),
    Acc = ordsets:new(),
    ResAcc = msg_store_collect(SubscriberId, Pids, Acc),
    ParentPid ! {self(), Ref, ordsets:to_list(ResAcc)}.

msg_store_collect(_, [], Acc) -> Acc;
msg_store_collect(SubscriberId, [Pid|Rest], Acc) ->
    Res = gen_server:call(Pid, {find_for_subscriber_id, SubscriberId}, infinity),
    msg_store_collect(SubscriberId, Rest, ordsets:union(Res, Acc)).

get_ref(BucketPid) ->
    gen_server:call(BucketPid, get_ref).

refcount(MsgRef) ->
    call(MsgRef, {refcount, MsgRef}).

call(Key, Req) ->
    case vmq_lmdb_store_sup:get_bucket_pid(Key) of
        {ok, BucketPid} ->
            gen_server:call(BucketPid, Req, infinity);
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([{InstanceId,Env}]) ->
    %% Initialize random seed
    rnd:seed(os:timestamp()),

    Opts = vmq_config:get_env(msg_store_opts, []),
    S0 = #state{},
    process_flag(trap_exit, true),
    Name = integer_to_binary(InstanceId),
    case open_db(Opts, S0, Name, Env) of
        {ok, State} ->
            case check_store(State) of
                0 -> ok; % no unreferenced images
                N ->
                    lager:info("found and deleted ~p unreferenced messages in msg store instance ~p",
                               [N, InstanceId])
            end,
            %% Register Bucket Instance with the Bucket Registry
            vmq_lmdb_store_sup:register_bucket_pid(InstanceId, self()),
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_ref, _From, #state{ref=Ref} = State) ->
    {reply, Ref, State};
handle_call({refcount, MsgRef}, _From, State) ->
    RefCount =
    case ets:lookup(State#state.refs, MsgRef) of
        [] -> 0;
        [{_, Cnt}] -> Cnt
    end,
    {reply, RefCount, State};
handle_call(Request, _From, State) ->
    
    {reply, handle_req(Request, State), State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{ref={_Env,_Dbi}}) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
open_db(_Opts, State0, Name, Env) ->
    case elmdb:db_open(Env, Name, [create]) of
        {ok, Dbi} ->
            {ok, State0#state{ref = {Env, Dbi}}};
        {error, Reason} ->
            {error, Reason}
    end.

handle_req({write, {MP, _} = SubscriberId,
            #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                     routing_key=RoutingKey, payload=Payload}},
           #state{ref={Env,Bucket}, refs=Refs}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    RefKey = sext:encode({msg, MsgRef, SubscriberId}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    IdxVal = term_to_binary({os:timestamp(), Dup, QoS}),
    case incr_ref(Refs, MsgRef) of
        1 ->
            %% new message
            Val = term_to_binary({RoutingKey, Payload}),
            %% XXX-TODO: Maybe these should be put into a transaction
            %% - maybe optimizations exists in that case.
            {ok, Txn} = elmdb:txn_begin(Env),
            elmdb:txn_put(Txn, Bucket, MsgKey, Val),
            elmdb:txn_put(Txn, Bucket, RefKey, <<>>),
            elmdb:txn_put(Txn, Bucket, IdxKey, IdxVal),
            ok = elmdb:txn_commit(Txn);
        _ ->
            %% only write ref
            %% XXX-TODO: Maybe these should be put into a transaction
            %% - maybe optimizations exists in that case.
            {ok, Txn} = elmdb:txn_begin(Env),
            elmdb:txn_put(Txn, Bucket, RefKey, <<>>),
            elmdb:txn_put(Txn, Bucket, IdxKey, IdxVal),
            ok = elmdb:txn_commit(Txn)
    end;
handle_req({read, {MP, _} = SubscriberId, MsgRef},
           #state{ref={_,Bucket}}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    case elmdb:get(Bucket, MsgKey) of
        {ok, Val} ->
            {RoutingKey, Payload} = binary_to_term(Val),
            case elmdb:get(Bucket, IdxKey) of
                {ok, IdxVal} ->
                    {_TS, Dup, QoS} = binary_to_term(IdxVal),
                    Msg = #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                                   routing_key=RoutingKey, payload=Payload, persisted=true},
                    {ok, Msg};
                not_found ->
                    {error, idx_val_not_found}
            end;
        not_found ->
            {error, not_found}
    end;
handle_req({delete, {MP, _} = SubscriberId, MsgRef},
           #state{ref={Env,Bucket}, refs=Refs}) ->
    MsgKey = sext:encode({msg, MsgRef, {MP, ''}}),
    RefKey = sext:encode({msg, MsgRef, SubscriberId}),
    IdxKey = sext:encode({idx, SubscriberId, MsgRef}),
    case decr_ref(Refs, MsgRef) of
        not_found ->
            lager:warning("delete failed ~p due to not found", [MsgRef]);
        0 ->
            %% last one to be deleted
            %% XXX-TODO: Maybe these should be put into a transaction
            %% - maybe optimizations exists in that case.
            {ok, Txn} = elmdb:txn_begin(Env),
            elmdb:txn_delete(Txn, Bucket, RefKey),
            elmdb:txn_delete(Txn, Bucket, IdxKey),
            elmdb:txn_delete(Txn, Bucket, MsgKey),
            ok = elmdb:txn_commit(Txn);
        _ ->
            %% we have to keep the message, but can delete the ref and idx
            %% XXX-TODO: Maybe these should be put into a transaction
            %% - maybe optimizations exists in that case.
            {ok, Txn} = elmdb:txn_begin(Env),
            elmdb:txn_delete(Txn, Bucket, IdxKey),
            elmdb:txn_delete(Txn, Bucket, MsgKey),
            ok = elmdb:txn_commit(Txn)
    end;
handle_req({find_for_subscriber_id, SubscriberId},
           #state{ref={Env,Bucket}} = State) ->
    {ok, Txn} = elmdb:ro_txn_begin(Env),
    {ok, Cur} = elmdb:ro_txn_cursor_open(Txn, Bucket),

    FirstIdxKey = sext:encode({idx, SubscriberId, ''}),
    {TxnAction, Acc} = iterate_index_items(elmdb:ro_txn_cursor_get(Cur, {set, FirstIdxKey}),
                                           SubscriberId, ordsets:new(), Cur, State),
    ok = elmdb:ro_txn_cursor_close(Cur),
    case TxnAction of
        commit ->
            ok = elmdb:ro_txn_commit(Txn);
        abort ->
            ok = elmdb:ro_txn_abort(Txn)
    end,
    Acc.

iterate_index_items({error, _}, _, Acc, _, _) ->
    %% XXX-TODO: not sure if abort/commit makes sense for read
    %% transactions at all.
    {abort, Acc};
iterate_index_items(not_found, _, Acc, _, _) ->
    {commit, Acc};
iterate_index_items({ok, IdxKey, IdxVal}, SubscriberId, Acc, Cur, State) ->
    case sext:decode(IdxKey) of
        {idx, SubscriberId, MsgRef} ->
            {TS, _Dup, _QoS} = binary_to_term(IdxVal),
            iterate_index_items(elmdb:ro_txn_cursor_get(Cur, next), SubscriberId,
                                ordsets:add_element({TS, MsgRef}, Acc), Cur, State);
        _ ->
            %% all message refs accumulated for this subscriber
            {commit, Acc}
    end.


check_store(#state{ref={Env,Bucket}, refs=Refs}) ->
    {ok, Txn} = elmdb:ro_txn_begin(Env),
    {ok, Cur} = elmdb:ro_txn_cursor_open(Txn, Bucket),
    Res = check_store(Bucket, Refs, elmdb:ro_txn_cursor_get(Cur, first), Cur,
                {undefined, undefined, true}, 0),
    ok = elmdb:ro_txn_cursor_close(Cur),
    ok = elmdb:ro_txn_commit(Txn),    
    Res.

check_store(Bucket, Refs, {ok, Key, _}, Cur, {PivotMsgRef, PivotMP, HadRefs} = Pivot, N) ->
    {NewPivot, NewN} =
    case sext:decode(Key) of
        {msg, PivotMsgRef, {PivotMP, ClientId}} when ClientId =/= '' ->
            %% Inside the 'same' Message Section. This means we have found refs
            %% for this message -> no delete required.
            {{PivotMsgRef, PivotMP, true}, N};
        {msg, NewPivotMsgRef, {NewPivotMP, ''}} when HadRefs ->
            %% New Message Section started, previous section included refs
            %% -> no delete required
            {{NewPivotMsgRef, NewPivotMP, false}, N}; %% challenge the new message
        {msg, NewPivotMsgRef, {NewPivotMP, ''}} -> % HadRefs == false
            %% New Message Section started, previous section didn't include refs
            %% -> delete required
            elmdb:delete(Bucket, Key),
            {{NewPivotMsgRef, NewPivotMP, false}, N + 1}; %% challenge the new message
        {idx, _, MsgRef} ->
            incr_ref(Refs, MsgRef),
            {Pivot, N};
        Entry ->
            lager:warning("inconsistent message store entry detected: ~p", [Entry]),
            {Pivot, N}
    end,
    check_store(Bucket, Refs, elmdb:ro_txn_cursor_get(Cur, next), Cur, NewPivot, NewN);
check_store(Bucket, _, not_found, _, {PivotMsgRef, PivotMP, false}, N) ->
    %% XXX-TODO: It's not clear what the error coresponding to
    %% 'invalid_iterator' in leveldb is.
    Key = sext:encode({msg, PivotMsgRef, {PivotMP, ''}}),
    elmdb:delete(Bucket, Key),
    N + 1;
check_store(_Bucket, _, not_found, _, {_, _, true}, N) ->
    N.

incr_ref(Refs, MsgRef) ->
    case ets:insert_new(Refs, {MsgRef, 1}) of
        true -> 1;
        false ->
            ets:update_counter(Refs, MsgRef, 1)
    end.

decr_ref(Refs, MsgRef) ->
    try ets:update_counter(Refs, MsgRef, -1) of
        0 ->
            ets:delete(Refs, MsgRef),
            0;
        V ->
            V
    catch
        _:_ ->
            not_found
    end.
