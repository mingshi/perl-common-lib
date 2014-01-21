package M::Base;
use Mojo::Base -base;
use MY::Utils;
use Clone qw(clone);

has database => undef;
has table => undef;

sub resultset { R($_[0]->table, $_[0]->database) }

sub singleton { 
    state %instances;

    my $class = ref $_[0] || $_[0];

    $instances{$class} //= $class->SUPER::new; 
}

sub get {
    my ($self, $ids, $cache) = @_;
    
    $cache //= {};

    $cache = ($cache->{__CACHE}{$self->database . '.' . $self->table} //= {});

    my $is_multiple = 1;

    unless (ref $ids ~~ 'ARRAY') {
        $is_multiple = 0;
        $ids = [ $ids ];
    }

    my %res = ();
    my @id_not_in_cache = ();

    for my $id (@$ids) {
        if (exists $cache->{$id}) {
            $res{$id} = clone $cache->{$id};
            next;
        }
        
        push @id_not_in_cache, $id;
    }

    if (@id_not_in_cache) {
        my @rows = $self->select({
            id => \@id_not_in_cache,
        })->all;

        for my $row (@rows) {
            $cache->{$row->id} = $row->hashref;
            $res{$row->id} = clone $cache->{$row->id};
        }
    }

    unless ($is_multiple) {
        return $res{$ids->[0]};
    }

    return \%res;
}

sub select {
    my ($self, $where, $attrs) = @_;
    
    $where //= {};
    $attrs //= [];

    my $rs = $self->resultset;

    if ((ref $where eq 'HASH' and keys %$where) or 
        (ref $where eq 'ARRAY' and @$where)
    ) {
        $rs = $rs->search($where);
    }

    if (ref $attrs eq 'HASH') {
        my @attrs = map {
            $_ => $attrs->{$_}
        } keys %$attrs;

        $attrs = \@attrs;
    }

    my @methods = qw{
        select order_by group_by inner_join 
        left_join limit offset having
        page rows_per_page
    };

    for (my $i = 0; $i <= $#{$attrs}; $i += 2) {
        my $method = $attrs->[$i];
        my $params = $attrs->[$i + 1];

        if ($method ~~ @methods) {
            $rs = $rs->$method(
                ref $params ~~ 'ARRAY' ? @$params : $params 
            );
        }
    }
    
    return $rs;
}

sub select_hashref_rows {
    my $rs = shift->select(@_);

    my @rows = map { $_->hashref } $rs->all;

    return @rows;
}

sub select_count {
    return shift->select(@_)->count; 
}

sub insert_replace {
    my ($self, $key, $ins, $upt) = @_;

    for (1..3) {
        if (my $row = $self->find($key)) {
            $row->update($upt);
            undef $@;
            last;
        } else {
            eval { 
                $self->insert({
                    %$key,
                    %$ins
                }); 
            };

            if ($@ ~~ /Duplicate/) {
                next;
            }

            last;
        }
    }

    return $self;
}

{
    no strict 'refs';

    for my $method (qw/update delete/) {
        *{__PACKAGE__ . '::' . $method} = sub {
            my ($self, $where, $data) = @_;

            unless (ref $where) {
                if (my $obj = $self->resultset->find($where)) {
                    return $obj->$method($data);
                }

                return 0;
            }

            return $self->resultset->search($where)->$method($data);
        };
    }

    for my $method (qw/find find_or_insert insert/) {
        *{__PACKAGE__ . '::' . $method} = sub {
            shift->resultset->$method(@_); 
        };
    }
}

sub _enum {
    my ($package, $name, $hash) = @_;     

    my @keys = keys %$hash;

    my @options = map {
        { name => $hash->{$_}, value => $_ };
    } @keys;

    Mojo::Base::attr($package, "${name}_list" => sub { \@keys });

    Mojo::Base::attr($package, "${name}_map" => sub { $hash });

    Mojo::Base::attr($package, "${name}_options" => sub { \@options });
}
1;
