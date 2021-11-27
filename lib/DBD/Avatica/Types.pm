package DBD::Avatica::Types;

use strict;
use warnings;

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

1;
