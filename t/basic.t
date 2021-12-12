use strict;
use warnings;
use Test::More;

use lib 't/lib';
use Mock 'mock_method';

use DBI;
use DBD::Avatica;

my $is_mock = !($ENV{TEST_ONLINE});
my $url = $ENV{TEST_ONLINE} || 'http://127.0.0.1:8765';

&mock_static if $is_mock;
my $dbh = DBI->connect("dbi:Avatica:adapter_name=phoenix;url=$url");
my $ret = $dbh->do(q{DROP TABLE IF EXISTS test});
is $ret, '0E0', 'check drop res';
$ret = $dbh->do(q{CREATE TABLE test(id BIGINT PRIMARY KEY, text VARCHAR)});
is $ret, '0E0', 'check drop res';

subtest "prepare & execute without params" => sub {
    if ($is_mock) {
        mock_prepare_seq([
            q!{"statement":{"connectionId":"yylc41tx9whb7d7h996rzq8k7q7cv2","id":87,"signature":{"sql":"UPSERT INTO test VALUES (1, 'foo')","cursorFactory":{"style":"LIST"}}},"metadata":{"serverAddress":"c497a18abde6:8765"}}!,
            q!{"statement":{"connectionId":"yylc41tx9whb7d7h996rzq8k7q7cv2","id":89,"signature":{"columns":[{"searchable":true,"signed":true,"displaySize":40,"label":"ID","columnName":"ID","tableName":"TEST","readOnly":true,"columnClassName":"java.lang.Long","type":{"id":4294967291,"name":"BIGINT","rep":"PRIMITIVE_LONG"}},{"ordinal":1,"searchable":true,"nullable":1,"displaySize":40,"label":"TEXT","columnName":"TEXT","tableName":"TEST","readOnly":true,"columnClassName":"java.lang.String","type":{"id":12,"name":"VARCHAR","rep":"STRING"}}],"sql":"SELECT * FROM test","cursorFactory":{"style":"LIST"}}},"metadata":{"serverAddress":"c497a18abde6:8765"}}!
        ]);
        mock_execute_seq([
            q!{"results":[{"connectionId":"yylc41tx9whb7d7h996rzq8k7q7cv2","statementId":88,"updateCount":1,"metadata":{"serverAddress":"c497a18abde6:8765"}}],"metadata":{"serverAddress":"c497a18abde6:8765"}}!,
            q!{"results":[{"connectionId":"yylc41tx9whb7d7h996rzq8k7q7cv2","statementId":90,"ownStatement":true,"signature":{"columns":[{"searchable":true,"signed":true,"displaySize":40,"label":"ID","columnName":"ID","tableName":"TEST","readOnly":true,"columnClassName":"java.lang.Long","type":{"id":4294967291,"name":"BIGINT","rep":"PRIMITIVE_LONG"}},{"ordinal":1,"searchable":true,"nullable":1,"displaySize":40,"label":"TEXT","columnName":"TEXT","tableName":"TEST","readOnly":true,"columnClassName":"java.lang.String","type":{"id":12,"name":"VARCHAR","rep":"STRING"}}],"cursorFactory":{"style":"LIST"}},"firstFrame":{"done":true,"rows":[{"value":[{"value":[{"type":"LONG","numberValue":1}],"scalarValue":{"type":"LONG","numberValue":1}},{"value":[{"type":"STRING","stringValue":"foo"}],"scalarValue":{"type":"STRING","stringValue":"foo"}}]}]},"updateCount":18446744073709551615,"metadata":{"serverAddress":"c497a18abde6:8765"}}],"metadata":{"serverAddress":"c497a18abde6:8765"}}!
        ]);
    }

    my $sth = $dbh->prepare(q{UPSERT INTO test VALUES (1, 'foo')});
    isnt $sth, undef, 'sth is defined';

    $ret = $sth->execute;
    is $ret, 1, 'number of inserted rows';

    $sth = $dbh->prepare(q{SELECT * FROM test});
    isnt $sth, undef, 'sth is defined';

    $ret = $sth->execute;
    is $ret, 1, 'execute is successfully';

    my $row = $sth->fetchrow_arrayref;
    is_deeply $row, [1, 'foo'], 'check row';

    $row = $sth->fetchrow_arrayref;
    is $row, undef, 'no more rows';
};

# subtest "prepare & execute with params" => sub {
#     my $sth = $dbh->prepare(q{UPSERT INTO test VALUES (?, ?)});
#     isnt $sth, undef, 'sth is defined';

#     $ret = $sth->execute(2, 'bar');
#     is $ret, 1, 'number of inserted rows';

#     $sth = $dbh->prepare(q{SELECT * FROM test WHERE id = ?});
#     isnt $sth, undef, 'sth is defined';

#     $ret = $sth->execute(2);
#     is $ret, 1, 'execute is successfully';

#     my $row = $sth->fetchall_arrayref;
#     is_deeply $row, [[2, 'bar']], 'check rows';
# };


done_testing;

sub mock_static {
    &mock_connect;
    &mock_create_table;
    mock_call('close_statement', 'CloseStatementResponse', '{"metadata":{"serverAddress":"c497a18abde6:8765"}}');
    mock_call('close_connection', 'CloseConnectionResponse', '{"metadata":{"serverAddress":"c497a18abde6:8765"}}');
}

sub mock_connect {
    mock_call('open_connection', 'OpenConnectionResponse', '{"metadata":{"serverAddress":"c497a18abde6:8765"}}');
    mock_call('connection_sync', 'ConnectionSyncResponse', '{"connProps":{"autoCommit":true,"transactionIsolation":2,"hasAutoCommit":true,"hasReadOnly":true},"metadata":{"serverAddress":"c497a18abde6:8765"}}');
    mock_call('database_property', 'DatabasePropertyResponse', '{"props":[{"key":{"name":"GET_DATABASE_MAJOR_VERSION"},"value":{"type":"INTEGER","numberValue":4}},{"key":{"name":"GET_DEFAULT_TRANSACTION_ISOLATION"},"value":{"type":"INTEGER","numberValue":2}},{"key":{"name":"GET_NUMERIC_FUNCTIONS"},"value":{"type":"STRING"}},{"key":{"name":"GET_STRING_FUNCTIONS"},"value":{"type":"STRING"}},{"key":{"name":"GET_DRIVER_MINOR_VERSION"},"value":{"type":"INTEGER","numberValue":15}},{"key":{"name":"GET_DRIVER_VERSION"},"value":{"type":"STRING","stringValue":"4.15"}},{"key":{"name":"GET_DATABASE_PRODUCT_VERSION"},"value":{"type":"STRING","stringValue":"4.15"}},{"key":{"name":"AVATICA_VERSION"},"value":{"type":"STRING","stringValue":"1.18.0"}},{"key":{"name":"GET_DRIVER_MAJOR_VERSION"},"value":{"type":"INTEGER","numberValue":4}},{"key":{"name":"GET_SYSTEM_FUNCTIONS"},"value":{"type":"STRING"}},{"key":{"name":"GET_DRIVER_NAME"},"value":{"type":"STRING","stringValue":"PhoenixEmbeddedDriver"}},{"key":{"name":"GET_DATABASE_MINOR_VERSION"},"value":{"type":"INTEGER","numberValue":15}},{"key":{"name":"GET_DATABASE_PRODUCT_NAME"},"value":{"type":"STRING","stringValue":"Phoenix"}},{"key":{"name":"GET_TIME_DATE_FUNCTIONS"},"value":{"type":"STRING"}},{"key":{"name":"GET_S_Q_L_KEYWORDS"},"value":{"type":"STRING"}}],"metadata":{"serverAddress":"c497a18abde6:8765"}}');
}

sub mock_create_table {
    mock_prepare_seq([
        '{"statement":{"connectionId":"35p0fchr4g6az0wwy8l541zl0ys2yh","id":79,"signature":{"sql":"DROP TABLE IF EXISTS test","cursorFactory":{"style":"LIST"}}},"metadata":{"serverAddress":"c497a18abde6:8765"}}',
        '{"statement":{"connectionId":"35p0fchr4g6az0wwy8l541zl0ys2yh","id":81,"signature":{"sql":"CREATE TABLE test(id BIGINT PRIMARY KEY, text VARCHAR)","cursorFactory":{"style":"LIST"}}},"metadata":{"serverAddress":"c497a18abde6:8765"}}'
    ]);
    mock_execute_seq([
        '{"results":[{"connectionId":"35p0fchr4g6az0wwy8l541zl0ys2yh","statementId":80,"metadata":{"serverAddress":"c497a18abde6:8765"}}],"metadata":{"serverAddress":"c497a18abde6:8765"}}',
        '{"results":[{"connectionId":"35p0fchr4g6az0wwy8l541zl0ys2yh","statementId":82,"metadata":{"serverAddress":"c497a18abde6:8765"}}],"metadata":{"serverAddress":"c497a18abde6:8765"}}'
    ]);
}

sub mock_prepare_seq {
    my $list = shift;
    mock_call_seq('prepare', 'PrepareResponse', $list);
}

sub mock_execute_seq {
    my $list = shift;
    mock_call_seq('execute', 'ExecuteResponse', $list);
}

sub mock_call {
    my ($func, $class, $data) = @_;
    mock_method "Avatica::Client::$func", sub {
        return 1, "Avatica::Client::Protocol::$class"->decode_json($data);
    };
}

sub mock_call_seq {
    my ($func, $class, $data_list) = @_;
    my $count = 0;
    mock_method "Avatica::Client::$func", sub {
        my $data = $data_list->[$count++];
        return 1, "Avatica::Client::Protocol::$class"->decode_json($data);
    };
}
