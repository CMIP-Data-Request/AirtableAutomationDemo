from .__init__ import * 


key = os.environ.get('ATKEY')
dr = rr.LoadBase(key)

# print all bases
for base in dr.bases:
    print(base)

base = dr.bases[-1]

print('\n\nSelected base:', base.name)

# get the information
dr.load(base)
# dump it into a directory
dr.dump('./data/AT_dump')


# we can also store the raw dataframes as a pkl object for later use
atdata = dr.pkldump



parse_entry(atdata,idkey = 'Compound Name',entry = 'Variable',variable_dir = './data/variables/')
parse_entry(atdata,idkey = 'label', entry = 'Cell Methods',variable_dir = './data/cell-methods/')
parse_entry(atdata,idkey = 'Name', entry = 'Physical Parameter',variable_dir = './data/physical-parameter/')

