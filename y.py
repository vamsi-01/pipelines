from kfp import dsl

CONSTANT = 'foo'


# @dsl.component
def identity():
    import pdb
    pdb.set_trace()
    print(CONSTANT)


# subprocess.run(command=[], args=[])
identity()