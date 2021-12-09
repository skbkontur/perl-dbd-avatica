package DBD::Avatica;

use strict;
use warnings;
use DBI;
use vars qw($VERSION $err $errstr $sqlstate $drh);

$VERSION = '0.01.0';

$drh = undef;

sub driver {
    return $drh if $drh;
    my ($class, $attr) = @_;
    DBI->setup_driver('DBD::Avatica');
    $drh = DBI::_new_drh("${class}::dr", {
        'Name'          => 'Avatica',
        'Version'       => $VERSION,
        'Err'           => \$err,
        'Errstr'        => \$errstr,
        'State'         => \$sqlstate,
        'Attribution'   => "DBD::Avatica $VERSION by skbkontur team"
    });
    return $drh;
}

sub CLONE {
    $drh = undef;
}

# h - some handle, it may be drh, dbh, sth
sub _client {
    my ($h, $method) = (shift, shift);

    my $client = $h->FETCH('avatica_client');
    return unless $client;

    my $connection_id = $h->FETCH('avatica_connection_id');

    local $SIG{PIPE} = "IGNORE";

    my ($ret, $response) = $client->$method($connection_id // (), @_);

    unless ($ret) {
        if ($response->{protocol}) {
            my ($err, $msg, $state) =  @{$response->{protocol}}{qw/error_code message sql_state/};
            my $status = $response->{http_status};
            $msg = "http status $status, error code $err, sql state $state" unless $msg;
            $h->set_err($err, $msg, $state);
        } else {
            $h->set_err(1, $response->{message});
        }
    }

    return ($ret, $response);
}

package DBD::Avatica::dr;

our $imp_data_size = 0;

use strict;
use warnings;

use DBI;
use Avatica::Client;

*_client = \&DBD::Avatica::_client;

sub connect {
    my ($drh, $dsn, $user, $pass, $attr) = @_;

    my %dsn = split /[;=]/, $dsn;

    my $adapter_name = ucfirst $dsn{adapter_name} // '';
    return $drh->set_err(1, q{Parameter "adapter_name" is required in dsn}) unless $adapter_name;
    my $adapter_class_path = "DBD/Avatica/Adapter/${adapter_name}.pm";
    my $adapter_class = "DBD::Avatica::Adapter::${adapter_name}";
    return $drh->set_err(1, qq{Adapter for adapter_name param $adapter_name not found}) unless eval { require $adapter_class_path; 1};

    my $url = $dsn{url};
    $url = 'http://' . $dsn{'hostname'} . ':' . $dsn{'port'} if !$url && $dsn{'hostname'} && $dsn{'port'};
    return $drh->set_err(1, q{Missing "url" parameter}) unless $url;

    $drh->{avatica_url} = $url;

    my %client_params;
    $client_params{ua} = delete $attr->{UserAgent} if $attr->{UserAgent};
    $client_params{max_retries} = delete $attr->{MaxRetries} if $attr->{MaxRetries};

    my $client = Avatica::Client->new(url => $url, %client_params);
    my $connection_id = _random_str();

    $drh->{avatica_client} = $client;
    my ($ret, $response) = _client($drh, 'open_connection', $connection_id);
    $drh->{avatica_client} = undef;

    return unless $ret;

    my ($outer, $dbh) = DBI::_new_dbh($drh, {
        'Name' => "${adapter_name};${url}"
    });

    my $adapter = $adapter_class->new(dbh => $dbh);
    $dbh->{avatica_adapter} = $adapter;

    $dbh->{avatica_pid} = $$;

    $dbh->STORE(Active => 1);

    $dbh->{avatica_client} = $client;
    $dbh->{avatica_connection_id} = $connection_id;
    my $connections = $drh->{avatica_connections} || [];
    push @$connections, $dbh;
    $drh->{avatica_connections} = $connections;

    for (qw/AutoCommit ReadOnly TransactionIsolation Catalog Schema/) {
        $dbh->{$_} = delete $attr->{$_} if exists $attr->{$_};
    }
    DBD::Avatica::db::_sync_connection_params($dbh);
    DBD::Avatica::db::_load_database_properties($dbh);

    $outer;
}

sub data_sources {
    my $drh = shift;
    my $url = $drh->{avatica_url};
    return "dbi:Avatica:url=$url";
}

sub disconnect_all {
    my $drh = shift;
    my $connections = $drh->{avatica_connections};
    return unless $connections && @$connections;

    my ($dbh, $name);
    while ($dbh = shift @$connections) {
        $name = $dbh->{Name};
        $drh->trace_msg("Disconnecting $name\n", 3);
        $dbh->disconnect();
    }
}

sub _random_str {
    my @alpha = ('0' .. '9', 'a' .. 'z');
    return join '', @alpha[ map { rand scalar(@alpha) } 1 .. 30 ];
}

sub STORE {
    my ($drh, $attr, $value) = @_;
    if ($attr =~ m/^avatica_/) {
        $drh->{$attr} = $value;
        return 1;
    }
    return $drh->SUPER::STORE($attr, $value);
}

sub FETCH {
    my ($drh, $attr) = @_;
    if ($attr =~ m/^avatica_/) {
        return $drh->{$attr};
    }
    return $drh->SUPER::FETCH($attr);
}

package DBD::Avatica::db;

our $imp_data_size = 0;

use strict;
use warnings;

use DBI;

*_client = \&DBD::Avatica::_client;

sub prepare {
    my ($dbh, $statement, $attr) = @_;

    my ($ret, $response) = _client($dbh, 'prepare', $statement);
    return unless $ret;

    my $stmt = $response->get_statement;
    my $statement_id = $stmt->get_id;
    my $signature = $stmt->get_signature;

    my ($outer, $sth) = DBI::_new_sth($dbh, {'Statement' => $statement});

    $sth->STORE(NUM_OF_PARAMS => $signature->parameters_size);
    $sth->STORE(NUM_OF_FIELDS => undef);

    $sth->{avatica_client} = $dbh->FETCH('avatica_client');
    $sth->{avatica_connection_id} = $dbh->FETCH('avatica_connection_id');
    $sth->{avatica_statement_id} = $statement_id;
    $sth->{avatica_signature} = $signature;
    $sth->{avatica_params} = $signature->get_parameters_list;
    $sth->{avatica_rows} = -1;
    $sth->{avatica_bind_params} = [];
    $sth->{avatica_data_done} = 1;
    $sth->{avatica_data} = [];

    $outer;
}

sub begin_work {
    my $dbh = shift;
    $dbh->{avatica_autocommit_at_begin_work} = $dbh->{AutoCommit};
    return 1 unless $dbh->{AutoCommit};
    $dbh->{AutoCommit} = 0;
    return _sync_connection_params($dbh);
}

sub commit {
    my $dbh = shift;
    return 1 if $dbh->{AutoCommit};
    my ($ret, $response) = _client($dbh, 'commit');
    return $ret unless $dbh->{avatica_autocommit_at_begin_work};
    $dbh->{AutoCommit} = 1;
    unless (_sync_connection_params($dbh)) {
        warn $dbh->errstr;
        # clear errors of setting autocomit = 1, because commit succeed
        $dbh->set_err(undef, undef, '');
    }
    return $ret;
}

sub rollback {
    my $dbh = shift;
    return 1 if $dbh->{AutoCommit};
    my ($ret, $response) = _client($dbh, 'rollback');
    return $ret unless $dbh->{avatica_autocommit_at_begin_work};
    $dbh->{AutoCommit} = 1;
    unless (_sync_connection_params($dbh)) {
        warn $dbh->errstr;
        # clear errors of setting autocomit = 1, because rollback succeed
        $dbh->set_err(undef, undef, '');
    }
    return $ret;
}

my %get_info_type = (
    ## Driver information:
     6 => ['SQL_DRIVER_NAME',                     'DBD::Avatica'            ],
     7 => ['SQL_DRIVER_VER',                      'DBD_VERSION'             ], # magic word
    14 => ['SQL_SEARCH_PATTERN_ESCAPE',           '\\'                      ],
    ## DBMS Information
    17 => ['SQL_DBMS_NAME',                       'DBMS_NAME'               ], # magic word
    18 => ['SQL_DBMS_VERSION',                    'DBMS_VERSION'            ], # magic word
    ## Data source information
    ## Supported SQL
   114 => ['SQL_CATALOG_LOCATION',                0                         ],
    41 => ['SQL_CATALOG_NAME_SEPARATOR',          ''                        ],
    28 => ['SQL_IDENTIFIER_CASE',                 1                         ], # SQL_IC_UPPER
    29 => ['SQL_IDENTIFIER_QUOTE_CHAR',           q{"}                      ],
    89 => ['SQL_KEYWORDS',                        'SQL_KEYWORDS'            ], # magic word
    ## SQL limits
    ## Scalar function information
    ## Conversion information - all but BIT, LONGVARBINARY, and LONGVARCHAR
);
for (keys %get_info_type) {
    $get_info_type{$get_info_type{$_}->[0]} = $get_info_type{$_};
}

sub get_info {
    my ($dbh, $type) = @_;
    my $res = $get_info_type{$type}[1];

    if (grep { $res eq $_ } 'DBMS_NAME', 'DBMS_VERSION', 'SQL_KEYWORDS') {
        _load_database_properties($dbh) unless $dbh->{avatica_info_type_cache};
        return $dbh->{avatica_info_type_cache}{$res};
    }

    if ($res eq 'DBD_VERSION') {
        my $v = $DBD::Avatica::VERSION;
        $v =~ s/_/./g; # 1.12.3_4 strip trial/dev symbols
        $v =~ s/[^0-9.]//g; # strip trial/dev symbols, a-la "-TRIAL" at the end
        return sprintf '%02d.%02d.%1d%1d%1d%1d', (split(/\./, "${v}.0.0.0.0.0.0"))[0..5];
    }

    return $res;
}

# returned columns:
# TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_NAME, SELF_REFERENCING_COL_NAME,
# REF_GENERATION, INDEX_STATE, IMMUTABLE_ROWS, SALT_BUCKETS, MULTI_TENANT, VIEW_STATEMENT, VIEW_TYPE,
# INDEX_TYPE, TRANSACTIONAL, IS_NAMESPACE_MAPPED, GUIDE_POSTS_WIDTH, TRANSACTION_PROVIDER
sub table_info {
    my $dbh = shift;
    my ($catalog, $schema, $table, $type) = @_;

    # minimum number of columns
    my $cols = ['TABLE_CAT', 'TABLE_SCHEM', 'TABLE_NAME', 'TABLE_TYPE', 'REMARKS'];

    if (
        defined $catalog && $catalog eq '%' &&
        defined $schema && $schema eq '' &&
        defined $table && $table eq ''
    ) {
        # returned columns: TABLE_CAT
        my ($ret, $response) = _client($dbh, 'catalog');
        return unless $ret;
        my $sth = _sth_from_result_set($dbh, 'table_info_catalog', $response);
        my $rows = $sth->fetchall_arrayref;
        push @$_, (undef) x 4 for @$rows; # fill to the minimum number of columns
        return _sth_from_data('table_info_catalog', $rows, $cols);
    }

    if (
        defined $catalog && $catalog eq '' &&
        defined $schema && $schema eq '%' &&
        defined $table && $table eq ''
    ) {
        # returned columns: TABLE_SCHEM, TABLE_CATALOG
        my ($ret, $response) = _client($dbh, 'schemas');
        return unless $ret;
        my $sth = _sth_from_result_set($dbh, 'table_info_schemas', $response);
        my $rows = $sth->fetchall_arrayref;
        $_ = [reverse(@$_), (undef) x 3] for @$rows; # fill to the minimum number of columns
        return _sth_from_data('table_info_schemas', $rows, $cols);
    }

    if (
        defined $catalog && $catalog eq '' &&
        defined $schema && $schema eq '' &&
        defined $table && $table eq '' &&
        defined $type && $type eq '%'
    ) {
        # returned columns: TABLE_TYPE
        my ($ret, $response) = _client($dbh, 'table_types');
        return unless $ret;
        my $sth = _sth_from_result_set($dbh, 'table_info_table_types', $response);
        my $rows = $sth->fetchall_arrayref;
        $_ = [(undef) x 3, @$_, undef] for @$rows; # fill to the minimum number of columns
        return _sth_from_data('table_info_table_types', $rows, $cols);
    }

    my ($ret, $response) = _client($dbh, 'tables', $catalog, $schema, $table, $type);
    return unless $ret;
    return _sth_from_result_set($dbh, 'table_info', $response);
}

# returned columns:
# TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH,
# DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB,
# CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
# SOURCE_DATA_TYPE, IS_AUTOINCREMENT, ARRAY_SIZE, COLUMN_FAMILY, TYPE_ID, VIEW_CONSTANT, MULTI_TENANT,
# KEY_SEQ
sub column_info {
    my $dbh = shift;
    my ($catalog, $schema, $table, $column) = @_;

    my ($ret, $response) = _client($dbh, 'columns', $catalog, $schema, $table, $column);
    return unless $ret;

    return _sth_from_result_set($dbh, 'column_info', $response);
}

# returned columns:
# TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME,
# ASC_OR_DESC, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, TYPE_ID, VIEW_CONSTANT
sub primary_key_info {
    my ($dbh, $catalog, $schema, $table) = @_;

    my ($ret, $response) = _client($dbh, 'primary_keys', $catalog, $schema, $table);
    return unless $ret;

    # extend signature with database specific columns
    $dbh->{avatica_adapter}->extend_primary_key_info_signature($response->get_signature);

    return _sth_from_result_set($dbh, 'primary_keys', $response);
}

sub foreign_key_info { }

sub statistics_info { }

sub type_info_all { [] }

sub _sth_from_data {
    my ($statement, $rows, $col_names, %attr) = @_;
    my $sponge = DBI->connect('dbi:Sponge:', '', '', { RaiseError => 1 });
    my $sth = $sponge->prepare($statement, { rows=>$rows, NAME=>$col_names, %attr });
    return $sth;
}

sub _sth_from_result_set {
    my ($dbh, $operation, $result_set) = @_;

    my $statement_id = $result_set->get_statement_id;
    my $signature = $result_set->get_signature;
    my $num_columns = $signature->columns_size;

    my ($outer, $sth) = DBI::_new_sth($dbh, {'Statement' => $operation});

    my $frame = $result_set->get_first_frame;
    $sth->{avatica_data_done} = $frame->get_done;
    $sth->{avatica_data} = $frame->get_rows_list;
    $sth->{avatica_rows} = 0;
    $sth->{avatica_client} = $dbh->{avatica_client};
    $sth->{avatica_connection_id} = $dbh->{avatica_connection_id};
    $sth->{avatica_statement_id} = $statement_id;
    $sth->{avatica_signature} = $signature;

    $sth->STORE(NUM_OF_FIELDS => $num_columns);
    $sth->STORE(Active => 1);

    $outer;
}

sub _sync_connection_params {
    my $dbh = shift;
    my %props = map { $_ => $dbh->{$_} }
                grep { exists $dbh->{$_} }
                qw/AutoCommit ReadOnly TransactionIsolation Catalog Schema/;

    my ($ret, $response) = _client($dbh, 'connection_sync', \%props);
    return unless $ret;

    my $props = $response->get_conn_props;
    $dbh->{AutoCommit} = $props->get_auto_commit if $props->has_auto_commit;
    $dbh->{ReadOnly} = $props->get_read_only if $props->has_read_only;
    $dbh->{TransactionIsolation} = $props->get_transaction_isolation;
    $dbh->{Catalog} = $props->get_catalog if $props->get_catalog;
    $dbh->{Schema} = $props->get_schema if $props->get_schema;
    return 1;
}

sub _load_database_properties {
    my $dbh = shift;
    my ($ret, $response) = _client($dbh, 'database_property');
    return unless $ret;
    my $props = $dbh->{avatica_adapter}->map_database_properties($response->get_props_list);
    $dbh->{$_} = $props->{$_} for qw/AVATICA_DRIVER_NAME AVATICA_DRIVER_VERSION/;
    $dbh->{avatica_info_type_cache}{$_} = $props->{$_} for qw/DBMS_NAME DBMS_VERSION SQL_KEYWORDS/;
}

sub disconnect {
    my $dbh = shift;
    return 1 unless $dbh->FETCH('Active');
    $dbh->STORE(Active => 0);

    if ($dbh->{avatica_pid} != $$) {
        $dbh->{avatica_client} = undef;
        return 1;
    }

    my ($ret, $response) = _client($dbh, 'close_connection');
    $dbh->{avatica_client} = undef;

    return $ret;
}

sub STORE {
    my ($dbh, $attr, $value) = @_;
    if (grep { $attr eq $_ } ('AutoCommit', 'ReadOnly', 'TransactionIsolation', 'Catalog', 'Schema')) {
        $dbh->{$attr} = $value;
        _sync_connection_params($dbh);
        return 1;
    }
    if ($attr =~ m/^avatica_/) {
        $dbh->{$attr} = $value;
        return 1;
    }
    return $dbh->SUPER::STORE($attr, $value);
}

sub FETCH {
    my ($dbh, $attr) = @_;
    if ($attr =~ m/^avatica_/) {
        return $dbh->{$attr};
    }
    if (grep { $attr eq $_ }
        qw/AutoCommit ReadOnly TransactionIsolation Catalog Schema AVATICA_DRIVER_NAME AVATICA_DRIVER_VERSION/) {
        return $dbh->{$attr};
    }
    return $dbh->SUPER::FETCH($attr);
}

sub DESTROY {
    my $dbh = shift;
    return unless $dbh->FETCH('Active');
    return if $dbh->FETCH('InactiveDestroy');
    eval { $dbh->disconnect() };
}

package DBD::Avatica::st;

our $imp_data_size = 0;

use strict;
use warnings;

use DBI;

use constant FETCH_SIZE => 2000;

*_client = \&DBD::Avatica::_client;


sub bind_param {
    my ($sth, $param, $value, $attr) = @_;

    # at the moment the type is not processed because we know type from prepare request
    # my ($type) = (ref $attr) ? $attr->{'TYPE'} : $attr;

    my $params = $sth->{avatica_bind_params};
    $params->[$param - 1] = $value;
    1;
}

sub execute {
    my ($sth, @bind_values) = @_;

    my $bind_params = $sth->{avatica_bind_params};
    @bind_values = @$bind_params if !@bind_values && $bind_params && @$bind_params;

    my $num_params = $sth->FETCH('NUM_OF_PARAMS');
    return $sth->set_err(1, 'Wrong number of parameters') if @bind_values != $num_params;

    my $statement_id = $sth->{avatica_statement_id};
    my $signature = $sth->{avatica_signature};

    my $dbh = $sth->{Database};
    my $mapped_params = $dbh->{avatica_adapter}->row_to_jdbc(\@bind_values, $sth->{avatica_params});

    my ($ret, $response) = _client($sth, 'execute', $statement_id, $signature, $mapped_params, FETCH_SIZE);
    unless ($ret) {
        return if $num_params != 0 || index($response->{message}, 'NullPointerException') == -1;

        # https://issues.apache.org/jira/browse/CALCITE-4900
        # so, workaround, if num_params == 0 then need to use create_statement && prepare_and_execute without params

        # clear errors
        $sth->set_err(undef, undef, '');

        my $sql = $sth->FETCH('Statement');

        ($ret, $response) = _client($sth, 'create_statement');
        return unless $ret;

        $statement_id = $sth->{avatica_statement_id} = $response->get_statement_id;

        ($ret, $response) = _client($sth, 'prepare_and_execute', $statement_id, $sql, undef, FETCH_SIZE);
        return unless $ret;
    }

    my $result = $response->get_results(0);

    if ($result->get_own_statement) {
        my $new_statement_id = $result->get_statement_id;
        _avatica_close_statement($sth) if $statement_id && $statement_id != $new_statement_id;
        $sth->{avatica_statement_id} = $new_statement_id;
    }

    $signature = $result->get_signature;
    $sth->{avatica_signature} = $signature;

    my $num_updates = $result->get_update_count;
    $num_updates = -1 if $num_updates == '18446744073709551615'; # max_int

    if ($num_updates >= 0) {
        # DML
        $sth->STORE(Active => 0);
        $sth->STORE(NUM_OF_FIELDS => 0);
        $sth->{avatica_rows} = $num_updates;
        $sth->{avatica_data_done} = 1;
        $sth->{avatica_data} = [];
        return $num_updates == 0 ? '0E0' : $num_updates;
    }

    # SELECT
    my $frame = $result->get_first_frame;
    $sth->{avatica_data_done} = $frame->get_done;
    $sth->{avatica_data} = $frame->get_rows_list;
    $sth->{avatica_rows} = 0;

    my $num_columns = $signature->columns_size;
    $sth->STORE(Active => 1);
    $sth->STORE(NUM_OF_FIELDS => $num_columns);

    return 1;
}

sub fetch {
    my ($sth) = @_;

    my $signature = $sth->{avatica_signature};

    my $avatica_rows_list = $sth->{avatica_data};
    my $avatica_rows_done = $sth->{avatica_data_done};

    if ((!$avatica_rows_list || !@$avatica_rows_list) && !$avatica_rows_done) {
        my $statement_id  = $sth->{avatica_statement_id};
        my ($ret, $response) = _client($sth, 'fetch', $statement_id, undef, FETCH_SIZE);
        return unless $ret;

        my $frame = $response->get_frame;
        $sth->{avatica_data_done} = $frame->get_done;
        $sth->{avatica_data} = $frame->get_rows_list;

        $avatica_rows_done = $sth->{avatica_data_done};
        $avatica_rows_list = $sth->{avatica_data};
    }

    if ($avatica_rows_list && @$avatica_rows_list) {
        $sth->{avatica_rows} += 1;
        my $dbh = $sth->{Database};
        my $avatica_row = shift @$avatica_rows_list;
        my $values = $avatica_row->get_value_list;
        my $columns = $signature->get_columns_list;
        my $row = $dbh->{avatica_adapter}->row_from_jdbc($values, $columns);
        return $sth->_set_fbav($row);
    }

    $sth->finish;
    return;
}
*fetchrow_arrayref = \&fetch;

sub rows {
    shift->{avatica_rows}
}

# It seems that here need to call _avatica_close_statement method,
# but then such a scenario will not work
# when there are many "execute" commands for one "prepare" command.
# Therefore, we will not do this here.
sub finish {
    my $sth = shift;
    $sth->STORE(Active => 0);
    1;
}

sub STORE {
    my ($sth, $attr, $value) = @_;
    if ($attr =~ m/^avatica_/) {
        $sth->{$attr} = $value;
        return 1;
    }
    return $sth->SUPER::STORE($attr, $value);
}

sub FETCH {
    my ($sth, $attr) = @_;
    if ($attr =~ m/^avatica_/) {
        return $sth->{$attr};
    }
    if ($attr eq 'NAME') {
        return $sth->{avatica_cache_name} ||=
            [map { $_->get_column_name } @{$sth->{avatica_signature}->get_columns_list}];
    }
    if ($attr eq 'TYPE') {
        my $dbh = $sth->{Database};
        return $sth->{avatica_cache_type} ||=
            [map { $dbh->{avatica_adapter}->to_dbi($_->get_type) } @{$sth->{avatica_signature}->get_columns_list}];
    }
    if ($attr eq 'PRECISION') {
        return $sth->{avatica_cache_precision} ||=
            [map { $_->get_display_size } @{$sth->{avatica_signature}->get_columns_list}];
    }
    if ($attr eq 'SCALE') {
        return $sth->{avatica_cache_scale} ||=
            [map { $_->get_scale || undef } @{$sth->{avatica_signature}->get_columns_list}];
    }
    if ($attr eq 'NULLABLE') {
        return $sth->{avatica_cache_nullable} ||=
            [map { $_->get_nullable} @{$sth->{avatica_signature}->get_columns_list}];
    }
    if ($attr eq 'ParamValues') {
        return $sth->{avatica_cache_paramvalues} ||=
            {map { $_ => ($sth->{avatica_bind_params}->[$_ - 1] // undef) } 1 .. @{$sth->{avatica_params} // []}};
    }
    return $sth->SUPER::FETCH($attr);
}

sub _avatica_close_statement {
    my $sth = shift;
    my $statement_id  = $sth->{avatica_statement_id};
    _client($sth, 'close_statement', $statement_id) if $statement_id && $sth->FETCH('Database')->{avatica_pid} == $$;
    $sth->{avatica_statement_id} = undef;
}

sub DESTROY {
    my $sth = shift;
    return if $sth->FETCH('InactiveDestroy');
    return unless $sth->FETCH('Database')->FETCH('Active');
    eval { _avatica_close_statement($sth) };
    $sth->finish;
}

1;
