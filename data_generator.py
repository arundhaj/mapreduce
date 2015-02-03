from random import randint

# open file
fo = open("random_numbers.txt", "a")

for i in range(0, 10000000):
    fo.write("%s\n" % randint(0, 1000))

# close file
fo.close()
