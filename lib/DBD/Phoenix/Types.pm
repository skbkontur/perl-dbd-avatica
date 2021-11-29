package DBD::Phoenix::Types;

use strict;
use warnings;
use DBI ':sql_types';

use Avatica::Types;

use parent 'Avatica::Types';

use constant JAVA_TO_REP => {
    %{Avatica::Types->JAVA_TO_REP()},

    # These are the non-standard types defined by Phoenix
    18  => Avatica::Client::Protocol::Rep::JAVA_SQL_TIME(),     # UNSIGNED_TIME
    19  => Avatica::Client::Protocol::Rep::JAVA_SQL_DATE(),     # UNSIGNED_DATE
    15  => Avatica::Client::Protocol::Rep::DOUBLE(),            # UNSIGNED_DOUBLE
    14  => Avatica::Client::Protocol::Rep::DOUBLE(),            # UNSIGNED_FLOAT
    9   => Avatica::Client::Protocol::Rep::INTEGER(),           # UNSIGNED_INT
    10  => Avatica::Client::Protocol::Rep::LONG(),              # UNSIGNED_LONG
    13  => Avatica::Client::Protocol::Rep::SHORT(),             # UNSIGNED_SMALLINT
    20  => Avatica::Client::Protocol::Rep::JAVA_SQL_TIMESTAMP(),    # UNSIGNED_TIMESTAMP
    11  => Avatica::Client::Protocol::Rep::BYTE(),              # UNSIGNED_TINYINT
};

use constant JAVA_TO_DBI => {
    -6  => SQL_TINYINT,                         # TINYINT
    5   => SQL_SMALLINT,                        # SMALLINT
    4   => SQL_INTEGER,                         # INTEGER
    -5  => SQL_BIGINT,                          # BIGINT
    6   => SQL_FLOAT,                           # FLOAT
    8   => SQL_DOUBLE,                          # DOUBLE
    2   => SQL_NUMERIC,                         # NUMERIC
    1   => SQL_CHAR,                            # CHAR
    91  => SQL_TYPE_DATE,                       # DATE
    92  => SQL_TYPE_TIME,                       # TIME
    93  => SQL_TYPE_TIMESTAMP,                  # TIMESTAMP
    -2  => SQL_BINARY,                          # BINARY
    -3  => SQL_VARBINARY,                       # VARBINARY
    16  => SQL_BOOLEAN,                         # BOOLEAN

    -7  => SQL_BIT,                             # BIT
    7   => SQL_REAL,                            # REAL
    3   => SQL_DECIMAL,                         # DECIMAL
    12  => SQL_VARCHAR,                         # VARCHAR
    -1  => SQL_LONGVARCHAR,                     # LONGVARCHAR
    -4  => SQL_LONGVARBINARY,                   # LONGVARBINARY
    2004  => SQL_BLOB,                          # BLOB
    2005  => SQL_CLOB,                          # CLOB
    -15 => SQL_CHAR,                            # NCHAR
    -9  => SQL_VARCHAR,                         # NVARCHAR
    -16 => SQL_LONGVARCHAR,                     # LONGNVARCHAR
    2011  => SQL_CLOB,                          # NCLOB
    2009  => SQL_LONGVARCHAR,                   # SQLXML
    2013 => SQL_TYPE_TIME_WITH_TIMEZONE,        # TIME_WITH_TIMEZONE
    2014 => SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,   # TIMESTAMP_WITH_TIMEZONE

    # Returned by Avatica for Arrays in EMPTY resultsets
    2000  => SQL_ARRAY,                         # JAVA_OBJECT
    2003  => SQL_ARRAY,                         # ARRAY
};

# params:
# class
# value
# Avatica::Client::Protocol::AvaticaParameter
sub to_jdbc {
    my ($class, $value, $avatica_param) = @_;

    my $jdbc_type_id = $avatica_param->get_parameter_type;

    # Phoenix add base 3000 for array types
    # https://github.com/apache/phoenix/blob/2a2d9964d29c2e47667114dbc3ca43c0e264a221/phoenix-core/src/main/java/org/apache/phoenix/schema/types/PDataType.java#L518
    my $is_array = $jdbc_type_id > 2900 && $jdbc_type_id < 3100;
    return $class->SUPER::to_jdbc($value, $avatica_param) unless $is_array && defined $value;

    # Phoenix added arrays with base 3000

    my $element_rep = $class->convert_jdbc_to_rep_type($jdbc_type_id - 3000);

    my $elem_avatica_param = Avatica::Client::Protocol::AvaticaParameter->new;
    $elem_avatica_param->set_parameter_type($jdbc_type_id - 3000);

    my $typed_value = Avatica::Client::Protocol::TypedValue->new;
    $typed_value->set_null(0);
    $typed_value->set_type(Avatica::Client::Protocol::Rep::ARRAY());
    $typed_value->set_component_type($element_rep);

    for my $v (@$value) {
        my $tv = $class->SUPER::to_jdbc($v, $elem_avatica_param);
        $typed_value->add_array_value($tv);
    }

    return $typed_value;
}

# params:
# class
# Avatica::Client::Protocol::AvaticaType
sub to_dbi {
    my ($class, $avatica_type) = @_;
    my $java_type_id = $avatica_type->get_id;

    if ($java_type_id > 0x7FFFFFFF) {
        $java_type_id = -(($java_type_id ^ 0xFFFFFFFF) + 1);
    }

    # ARRAY (may be for bind params only)
    return SQL_ARRAY if $java_type_id > 2900 && $java_type_id < 3100;

    my $dbi_type_id = $class->JAVA_TO_DBI()->{$java_type_id};
    return $java_type_id unless $dbi_type_id;
    return $dbi_type_id;
}

1;
