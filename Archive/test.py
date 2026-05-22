s = 'the cattled is batteled with Rattled'
replacement = ['cat', 'rat', 'bat']

for sub in replacement:
	s = " ".join([sub if word.lower().startswith(sub) else word for word in s.split()])
	
print(s)