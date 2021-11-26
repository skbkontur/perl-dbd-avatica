package DBD::Avatica;

use strict;
use warnings;
use DBI;
use vars qw($VERSION $err $errstr $sqlstate $drh);

$VERSION = '0.001';

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
    my $url = $dsn{'url'};
    $url = 'http://' . $dsn{'hostname'} . ':' . $dsn{'port'} if !$url && $dsn{'hostname'} && $dsn{'port'};

    unless ($url) {
        $drh->set_err(1, q{Missing "url" parameter});
        return;
    }

    $drh->STORE(avatica_url => $url);

    my $client = Avatica::Client->new(url => $url);
    my $connection_id = _random_str();

    $drh->STORE(avatica_client => $client);
    my ($ret, $response) = _client($drh, 'open_connection', $connection_id);
    $drh->STORE(avatica_client => undef);

    return unless $ret;

    my ($outer, $dbh) = DBI::_new_dbh($drh, {
        'Name' => $url
    });

    $dbh->STORE(Active => 1);
    $dbh->STORE(avatica_client => $client);
    $dbh->STORE(avatica_connection_id => $connection_id);
    my $connections = $drh->FETCH('avatica_connections') || [];
    push @$connections, $dbh;
    $drh->STORE(avatica_connections => $connections);
    $outer;
}

sub data_sources {
    my $drh = shift;
    my $url = $drh->FETCH('avatica_url');
    return "dbi:Avatica:url=$url";
}

sub disconnect_all {
    my $drh = shift;
    my $connections = $drh->FETCH('avatica_connections');
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

    use Data::Dumper;
    print Dumper $response;
    return unless $ret;


    my $stmt = $response->get_statement;
    my $statement_id = $stmt->get_id;
    my $signature = $stmt->get_signature;

    my ($outer, $sth) = DBI::_new_sth($dbh, {'Statement' => $statement});

    $sth->STORE(NUM_OF_PARAMS => $signature->parameters_size);
    $sth->STORE(NUM_OF_FIELDS => undef);

    $sth->STORE(avatica_client => $dbh->FETCH('avatica_client'));
    $sth->STORE(avatica_connection_id => $dbh->FETCH('avatica_connection_id'));
    $sth->STORE(avatica_statement_id => $statement_id);
    $sth->STORE(avatica_signature => $signature);
    $sth->STORE(avatica_rows => -1);

    my $params = {};
    $sth->STORE(avatica_bind_params => $params);
    $params->{$_} = undef for 1 .. $signature->parameters_size;

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

sub table_info {
    my $dbh = shift;
    my ($catalog, $schema, $table, $type) = @_;

    my ($ret, $response) = _client($dbh, 'tables', $catalog, $schema, $table, $type);
    use Data::Dumper;
    print Dumper $response;
    return unless $ret;

    # returned columns:
    # TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_NAME, SELF_REFERENCING_COL_NAME,
    # REF_GENERATION, INDEX_STATE, IMMUTABLE_ROWS, SALT_BUCKETS, MULTI_TENANT, VIEW_STATEMENT, VIEW_TYPE
    # INDEX_TYPE, TRANSACTIONAL, IS_NAMESPACE_MAPPED, GUIDE_POSTS_WIDTH, TRANSACTION_PROVIDER

    my $statement_id = $response->get_statement_id;
    my $signature = $response->get_signature;
    my $num_columns = $signature->columns_size;

    my ($outer, $sth) = DBI::_new_sth($dbh, {'Statement' => 'table_info'});

    my $frame = $response->get_first_frame;
    $sth->{avatica_data_done} = $frame->get_done;
    $sth->{avatica_data} = $frame->get_rows_list;
    $sth->{avatica_rows} = 0;

    $sth->STORE(NUM_OF_PARAMS => 0);
    $sth->STORE(NUM_OF_FIELDS => $num_columns);
    $sth->STORE(Active => 1);

    $sth->STORE(avatica_client => $dbh->FETCH('avatica_client'));
    $sth->STORE(avatica_connection_id => $dbh->FETCH('avatica_connection_id'));
    $sth->STORE(avatica_statement_id => $statement_id);
    $sth->STORE(avatica_signature => $signature);

    return $outer;
}

sub disconnect {
    my $dbh = shift;
    return 1 unless $dbh->FETCH('Active');
    $dbh->STORE(Active => 0);

    my ($ret, $response) = _client($dbh, 'close_connection');
    $dbh->STORE(avatica_client => undef);

    return $ret;
}

sub STORE {
    my ($dbh, $attr, $value) = @_;
    if ($attr eq 'AutoCommit') {
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
    if ($attr eq 'AutoCommit') {
        return 1;
    }
    if ($attr =~ m/^avatica_/) {
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

use Avatica::Types;

use constant FETCH_SIZE => 2000;

*_client = \&DBD::Avatica::_client;

sub execute {
    my ($sth, @bind_values) = @_;

    my $num_params = $sth->FETCH('NUM_OF_PARAMS');
    return $sth->set_err(1, 'Wrong number of parameters') if @bind_values != $num_params;

    my $bind_params = $sth->FETCH('avatica_bind_params');
    $bind_params->{$_ + 1} = $bind_values[$_] for 0 .. scalar(keys %{$bind_params});

    my $statement_id = $sth->FETCH('avatica_statement_id');
    my $signature = $sth->FETCH('avatica_signature');

    my $mapped_params = Avatica::Types->row_to_jdbc(\@bind_values, $signature->get_parameters_list);

    my ($ret, $response) = _client($sth, 'execute', $statement_id, $signature, $mapped_params, FETCH_SIZE);

    use Data::Dumper;
    print Dumper $response;
    return unless $ret;

    my $result = $response->get_results(0);

    if ($result->get_own_statement) {
        my $new_statement_id = $result->get_statement_id;
        _avatica_close_statement($sth) if $statement_id && $statement_id != $new_statement_id;
        $sth->STORE(avatica_statement_id => $new_statement_id);
    }

    $signature = $result->get_signature;
    $sth->STORE(avatica_signature => $signature);

    my $frame = $result->get_first_frame;
    $sth->{avatica_data_done} = $frame->get_done;
    $sth->{avatica_data} = $frame->get_rows_list;

    my $num_columns = $signature->columns_size;
    my $num_updates = $result->get_update_count;
    $num_updates = -1 if $num_updates == '18446744073709551615'; # max_int

    if ($num_updates >= 0) {
        # DML
        $sth->STORE(Active => 0);
        $sth->STORE(NUM_OF_FIELDS => 0);
        $sth->{avatica_rows} = $num_updates;
        return if $num_updates == 0 ? '0E0' : $num_updates;
    }

    # SELECT
    $sth->STORE(Active => 1);
    $sth->STORE(NUM_OF_FIELDS => $num_columns);
    $sth->{avatica_rows} = 0;

    return 1;
}

sub fetch {
    my ($sth) = @_;

    print 'fetchrow_arrayref', $/;

    my $signature = $sth->FETCH('avatica_signature');

    my $avatica_rows_list = $sth->{avatica_data};
    my $avatica_rows_done = $sth->{avatica_data_done};

    if ((!$avatica_rows_list || !@$avatica_rows_list) && !$avatica_rows_done) {
        my $statement_id  = $sth->FETCH('avatica_statement_id');
        my ($ret, $response) = _client($sth, 'fetch', $statement_id, undef, FETCH_SIZE);

        use Data::Dumper;
        print Dumper $response;
        return unless $ret;

        my $frame = $response->get_frame;
        $sth->{avatica_data_done} = $frame->get_done;
        $sth->{avatica_data} = $frame->get_rows_list;

        $avatica_rows_done = $sth->{avatica_data_done};
        $avatica_rows_list = $sth->{avatica_data};
    }

    if ($avatica_rows_list && @$avatica_rows_list) {
        my $avatica_row = shift @$avatica_rows_list;
        my $values = $avatica_row->get_value_list;
        my $columns = $signature->get_columns_list;
        my $row = Avatica::Types->row_from_jdbc($values, $columns);
        return $sth->_set_fbav($row);
    }

    $sth->finish;
    return;
}
*fetchrow_arrayref = \&fetch;

sub rows {
    shift->{avatica_rows}
}

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
    if ($attr eq 'ParamValues') {
        return $sth->{avatica_bind_params};
    }
    return $sth->SUPER::FETCH($attr);
}

sub _avatica_close_statement {
    my $sth = shift;
    my $statement_id  = $sth->FETCH('avatica_statement_id');
    _client($sth, 'close_statement', $statement_id) if $statement_id;
    $sth->STORE(avatica_statement_id => undef);
}

sub DESTROY {
    my $sth = shift;
    return if $sth->FETCH('InactiveDestroy');
    return unless $sth->FETCH('Database')->FETCH('Active');
    eval { _avatica_close_statement($sth) };
    $sth->finish;
}

1;
