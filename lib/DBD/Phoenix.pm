package DBD::Phoenix;

use strict;
use warnings;
use DBI;
use vars qw($VERSION $err $errstr $sqlstate $drh);

$VERSION = '0.001';

$drh = undef;

sub driver {
    return $drh if $drh;
    my ($class, $attr) = @_;
    DBI->setup_driver('DBD::Phoenix');
    $drh = DBI::_new_drh("${class}::dr", {
        'Name'          => 'Phoenix',
        'Version'       => $VERSION,
        'Err'           => \$err,
        'Errstr'        => \$errstr,
        'State'         => \$sqlstate,
        'Attribution'   => "DBD::Phoenix $VERSION by skbkontur team"
    });
    return $drh;
}

sub CLONE {
    $drh = undef;
}

# h - some handle, it may be drh, dbh, sth
sub _client {
    my ($h, $method) = (shift, shift);

    my $client = $h->FETCH('phoenix_client');
    return unless $client;

    my $connection_id = $h->FETCH('phoenix_connection_id');

    local $SIG{PIPE} = "IGNORE";

    my ($ret, $response) = $client->$method($connection_id // (), @_);

    unless ($ret) {
        if ($response->{protocol}) {
            my ($err, $msg, $state) =  @{$response->{protocol}}{qw/error_code message sql_state/};
            $h->set_err($err, $msg, $state);
        } else {
            $h->set_err(1, $response->{message});
        }
    }

    return ($ret, $response);
}

package DBD::Phoenix::dr;

our $imp_data_size = 0;

use strict;
use warnings;

use DBI;
use Avatica::Client;

*_client = \&DBD::Phoenix::_client;

sub connect {
    my ($drh, $dsn, $user, $pass, $attr) = @_;

    my %dsn = split /[;=]/, $dsn;
    my $url = $dsn{'url'};
    $url = 'http://' . $dsn{'hostname'} . ':' . $dsn{'port'} if !$url && $dsn{'hostname'} && $dsn{'port'};

    unless ($url) {
        $drh->set_err(1, q{Missing "url" parameter});
        return;
    }

    $drh->STORE(phoenix_url => $url);

    my $client = Avatica::Client->new(url => $url);
    my $connection_id = _random_str();

    $drh->STORE(phoenix_client => $client);
    my ($ret, $response) = _client($drh, 'open_connection', $connection_id);
    $drh->STORE(phoenix_client => undef);

    return unless $ret;

    my ($outer, $dbh) = DBI::_new_dbh($drh, {
        'Name' => $url
    });

    $dbh->STORE(Active => 1);
    $dbh->STORE(phoenix_client => $client);
    $dbh->STORE(phoenix_connection_id => $connection_id);
    my $connections = $drh->FETCH('phoenix_connections') || [];
    push @$connections, $dbh;
    $drh->STORE(phoenix_connections => $connections);
    $outer;
}

sub data_sources {
    my $drh = shift;
    my $url = $drh->FETCH('phoenix_url');
    return "dbi:Phoenix:url=$url";
}

sub disconnect_all {
    my $drh = shift;
    my $connections = $drh->FETCH('phoenix_connections');
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
    if ($attr =~ m/^phoenix_/) {
        $drh->{$attr} = $value;
        return 1;
    }
    return $drh->SUPER::STORE($attr, $value);
}

sub FETCH {
    my ($drh, $attr) = @_;
    if ($attr =~ m/^phoenix_/) {
        return $drh->{$attr};
    }
    return $drh->SUPER::FETCH($attr);
}

package DBD::Phoenix::db;

our $imp_data_size = 0;

use strict;
use warnings;

use DBI;

*_client = \&DBD::Phoenix::_client;

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

    $sth->STORE(phoenix_client => $dbh->FETCH('phoenix_client'));
    $sth->STORE(phoenix_connection_id => $dbh->FETCH('phoenix_connection_id'));
    $sth->STORE(phoenix_statement_id => $statement_id);
    $sth->STORE(phoenix_signature => $signature);
    $sth->STORE(phoenix_rows => -1);

    $sth->STORE(phoenix_bind_params => []);

    $outer;
}

sub commit {
    my $dbh = shift;
    my ($ret, $response) = _client($dbh, 'commit');
    return $ret;
}

sub rollback {
    my $dbh = shift;
    my ($ret, $response) = _client($dbh, 'rollback');
    return $ret;
}

sub get_info {
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

    # add phoenix specific columns
    my $s = $response->get_signature;
    # The following are non-standard Phoenix extensions
    # This returns '\x00\x00\x00A' or '\x00\x00\x00D' , but that's consistent with Java
    $s->add_columns(Avatica::Client->_build_column_metadata(7, 'ASC_OR_DESC', 12));
    $s->add_columns(Avatica::Client->_build_column_metadata(8, 'DATA_TYPE', 5));
    $s->add_columns(Avatica::Client->_build_column_metadata(9, 'TYPE_NAME', 12));
    $s->add_columns(Avatica::Client->_build_column_metadata(10, 'COLUMN_SIZE', 5));
    $s->add_columns(Avatica::Client->_build_column_metadata(11, 'TYPE_ID', 5));
    $s->add_columns(Avatica::Client->_build_column_metadata(12, 'VIEW_CONSTANT', 12));

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
    $sth->{phoenix_data_done} = $frame->get_done;
    $sth->{phoenix_data} = $frame->get_rows_list;
    $sth->{phoenix_rows} = 0;

    $sth->STORE(NUM_OF_FIELDS => $num_columns);
    $sth->STORE(Active => 1);

    $sth->STORE(phoenix_client => $dbh->FETCH('phoenix_client'));
    $sth->STORE(phoenix_connection_id => $dbh->FETCH('phoenix_connection_id'));
    $sth->STORE(phoenix_statement_id => $statement_id);
    $sth->STORE(phoenix_signature => $signature);

    $outer;
}

sub disconnect {
    my $dbh = shift;
    return 1 unless $dbh->FETCH('Active');
    $dbh->STORE(Active => 0);

    my ($ret, $response) = _client($dbh, 'close_connection');
    $dbh->STORE(phoenix_client => undef);

    return $ret;
}

sub STORE {
    my ($dbh, $attr, $value) = @_;
    if ($attr eq 'AutoCommit') {
        return 1;
    }
    if ($attr =~ m/^phoenix_/) {
        $dbh->{$attr} = $value;
        return 1;
    }
    return $dbh->SUPER::STORE($attr, $value);
}

sub FETCH {
    my ($dbh, $attr) = @_;
    if ($attr eq 'AutoCommit') {
        return 1;
    }
    if ($attr =~ m/^phoenix_/) {
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

package DBD::Phoenix::st;

our $imp_data_size = 0;

use strict;
use warnings;

use DBI;

use DBD::Phoenix::Types;

use constant FETCH_SIZE => 2000;

*_client = \&DBD::Phoenix::_client;


sub bind_param {
    my ($sth, $param, $value, $attr) = @_;

    # at the moment the type is not processed
    # my ($type) = (ref $attr) ? $attr->{'TYPE'} : $attr;

    my $params = $sth->FETCH('phoenix_bind_params');
    $params->[$param - 1] = $value;
    1;
}

sub execute {
    my ($sth, @bind_values) = @_;

    my $bind_params = $sth->FETCH('phoenix_bind_params');
    @bind_values = @$bind_params if !@bind_values && $bind_params && @$bind_params;

    my $num_params = $sth->FETCH('NUM_OF_PARAMS');
    return $sth->set_err(1, 'Wrong number of parameters') if @bind_values != $num_params;

    my $statement_id = $sth->FETCH('phoenix_statement_id');
    my $signature = $sth->FETCH('phoenix_signature');

    my $mapped_params = DBD::Phoenix::Types->row_to_jdbc(\@bind_values, $signature->get_parameters_list);

    my ($ret, $response) = _client($sth, 'execute', $statement_id, $signature, $mapped_params, FETCH_SIZE);
    return unless $ret;

    my $result = $response->get_results(0);

    if ($result->get_own_statement) {
        my $new_statement_id = $result->get_statement_id;
        _phoenix_close_statement($sth) if $statement_id && $statement_id != $new_statement_id;
        $sth->STORE(phoenix_statement_id => $new_statement_id);
    }

    $signature = $result->get_signature;
    $sth->STORE(phoenix_signature => $signature);

    my $frame = $result->get_first_frame;
    $sth->{phoenix_data_done} = $frame->get_done;
    $sth->{phoenix_data} = $frame->get_rows_list;

    my $num_columns = $signature->columns_size;
    my $num_updates = $result->get_update_count;
    $num_updates = -1 if $num_updates == '18446744073709551615'; # max_int

    if ($num_updates >= 0) {
        # DML
        $sth->STORE(Active => 0);
        $sth->STORE(NUM_OF_FIELDS => 0);
        $sth->{phoenix_rows} = $num_updates;
        return if $num_updates == 0 ? '0E0' : $num_updates;
    }

    # SELECT
    $sth->STORE(Active => 1);
    $sth->STORE(NUM_OF_FIELDS => $num_columns);
    $sth->{phoenix_rows} = 0;

    return 1;
}

sub fetch {
    my ($sth) = @_;

    my $signature = $sth->FETCH('phoenix_signature');

    my $phoenix_rows_list = $sth->{phoenix_data};
    my $phoenix_rows_done = $sth->{phoenix_data_done};

    if ((!$phoenix_rows_list || !@$phoenix_rows_list) && !$phoenix_rows_done) {
        my $statement_id  = $sth->FETCH('phoenix_statement_id');
        my ($ret, $response) = _client($sth, 'fetch', $statement_id, undef, FETCH_SIZE);
        return unless $ret;

        my $frame = $response->get_frame;
        $sth->{phoenix_data_done} = $frame->get_done;
        $sth->{phoenix_data} = $frame->get_rows_list;

        $phoenix_rows_done = $sth->{phoenix_data_done};
        $phoenix_rows_list = $sth->{phoenix_data};
    }

    if ($phoenix_rows_list && @$phoenix_rows_list) {
        $sth->{phoenix_rows} += 1;
        my $phoenix_row = shift @$phoenix_rows_list;
        my $values = $phoenix_row->get_value_list;
        my $columns = $signature->get_columns_list;
        my $row = DBD::Phoenix::Types->row_from_jdbc($values, $columns);
        return $sth->_set_fbav($row);
    }

    $sth->finish;
    return;
}
*fetchrow_arrayref = \&fetch;

sub rows {
    shift->{phoenix_rows}
}

# It seems that here need to call _phoenix_close_statement method,
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
    if ($attr =~ m/^phoenix_/) {
        $sth->{$attr} = $value;
        return 1;
    }
    return $sth->SUPER::STORE($attr, $value);
}

sub FETCH {
    my ($sth, $attr) = @_;
    if ($attr =~ m/^phoenix_/) {
        return $sth->{$attr};
    }
    if ($attr eq 'NAME') {
        return $sth->{phoenix_cache_name} ||=
            [map { $_->get_column_name } @{$sth->{phoenix_signature}->get_columns_list}];
    }
    if ($attr eq 'TYPE') {
        return $sth->{phoenix_cache_type} ||=
            [map { DBD::Phoenix::Types->to_dbi($_->get_type) } @{$sth->{phoenix_signature}->get_columns_list}];
    }
    if ($attr eq 'PRECISION') {
        return $sth->{phoenix_cache_precision} ||=
            [map { $_->get_display_size } @{$sth->{phoenix_signature}->get_columns_list}];
    }
    if ($attr eq 'SCALE') {
        return $sth->{phoenix_cache_scale} ||=
            [map { $_->get_scale || undef } @{$sth->{phoenix_signature}->get_columns_list}];
    }
    if ($attr eq 'NULLABLE') {
        return $sth->{phoenix_cache_nullable} ||=
            [map { $_->get_nullable} @{$sth->{phoenix_signature}->get_columns_list}];
    }
    if ($attr eq 'ParamValues') {
        my $params = $sth->{phoenix_bind_params};
        return {map { $_ => $params->[$_] } @$params};
    }
    return $sth->SUPER::FETCH($attr);
}

sub _phoenix_close_statement {
    my $sth = shift;
    my $statement_id  = $sth->FETCH('phoenix_statement_id');
    _client($sth, 'close_statement', $statement_id) if $statement_id;
    $sth->STORE(phoenix_statement_id => undef);
}

sub DESTROY {
    my $sth = shift;
    return if $sth->FETCH('InactiveDestroy');
    return unless $sth->FETCH('Database')->FETCH('Active');
    eval { _phoenix_close_statement($sth) };
    $sth->finish;
}

1;
