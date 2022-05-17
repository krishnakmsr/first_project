import re
heroRegex = re.compile (r'Batman|Tina Fey')
mo1 = heroRegex.search('Batman and Tina Fey.')
print(mo1.group())


x=['\'','\"','^','GoogleCan u Just','Google,can u Just']

k='kri"shna"'
z=0
for i in x:
     if i in k:
         z=10


print(z)



k=k.replace('"','')
print(k)
