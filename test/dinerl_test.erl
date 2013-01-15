-module(dinerl_test).
-include_lib("eunit/include/eunit.hrl").

-define(HASH_TABLE, <<"dinerl-unit-test-table-hash">>).
-define(RANGE_TABLE, <<"dinerl-unit-test-table-range">>).

dinerl_test_() ->
    case file:consult(filename:join([code:priv_dir(dinerl), "aws_credentials.term"])) of
        {ok, Config} ->
            Setup =
                fun () ->
                        AccessKey = proplists:get_value(access_key, Config),
                        SecretAccessKey = proplists:get_value(secret_access_key,
                                                              Config),

                        inets:start(),
                        application:start(crypto),
                        application:start(public_key),
                        application:start(ssl),
                        application:start(lhttpc),
                        dinerl:setup(AccessKey, SecretAccessKey, "us-east-1a"),

                        %% Create tables if necessary
                        %% Wait for tables to become active
                        create_tables()

                end,

            Teardown = fun (_) ->
                               ok
                       end,

            {setup, Setup, Teardown,
             [
              ?_test(get_put())
             ]};
        {error, enoent} ->
            []
    end.

create_tables() ->
    {ok, {[{<<"TableNames">>, Tables}]}} = dinerl:list_tables(),

    Config = [{?HASH_TABLE,
               {[{<<"HashKeyElement">>, {[{<<"AttributeName">>, <<"key">>},
                                          {<<"AttributeType">>, <<"S">>}]}}]}},
              {?RANGE_TABLE,
               {[{<<"HashKeyElement">>, {[{<<"AttributeName">>, <<"hash">>},
                                          {<<"AttributeType">>, <<"S">>}]}},
                 {<<"RangeKeyElement">>, {[{<<"AttributeName">>, <<"range">>},
                                           {<<"AttributeType">>, <<"N">>}]}}]}}
              ],

    lists:foreach(
      fun ({Name, Key}) ->
              case lists:member(Name, Tables) of
                  true ->
                      wait_for_active(Name),
                      ok;
                  false ->
                      case dinerl:create_table(Name, Key, 10, 5) of
                          {ok, _} ->
                              wait_for_active(Name),
                              ok;
                          {error, _, Reason} ->
                              throw({create_table_failed, Reason})
                      end
              end
      end, Config).

wait_for_active(Name) ->
    case dinerl:describe_table(Name) of
        {ok, {Response}} ->
            {Table} = proplists:get_value(<<"Table">>, Response),
            case proplists:get_value(<<"TableStatus">>, Table) of
                <<"ACTIVE">> ->
                    ok;
                _ ->
                    error_logger:info_msg("Waiting for table ~p..~n", [Name]),
                    timer:sleep(10000),
                    wait_for_active(Name)
            end;
        {error, _, Reason} ->
            throw({wait_for_active_failed, Reason})
    end.



%%
%% TESTS
%%

get_put() ->
    Rand = base64:encode(crypto:rand_bytes(10)),
    Key = {[{<<"HashKeyElement">>,
             {[{<<"S">>, Rand}]}}]},

    Item = {[
             {<<"int">>, {[{<<"N">>, <<"123">>}]}},
             {<<"key">>, {[{<<"S">>, Rand}]}},
             {<<"foo">>, {[{<<"S">>, <<"bar">>}]}}
            ]},

    ?assertEqual({ok, {[{<<"WritesUsed">>, 1}]}},
                 dinerl:delete_item(?HASH_TABLE, Key, [])),

    ?assertEqual({ok, {[{<<"ReadsUsed">>, 1}]}},
                 dinerl:get_item(?HASH_TABLE, Key, [])),

    ?assertEqual({ok, {[{<<"WritesUsed">>, 1}]}},
                 dinerl:put_item(?HASH_TABLE, Item, [])),

    ?assertEqual({ok, {[{<<"ReadsUsed">>, 1},
                        {<<"Item">>, Item}]}},
                 dinerl:get_item(?HASH_TABLE, Key, [{consistent, true}])).

