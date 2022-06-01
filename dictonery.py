keys = ('name', 'age', 'food')
values = ('Monty', 42, 'spam')

dic = {k:v for k,v in zip(keys, values)}

print(dic)
