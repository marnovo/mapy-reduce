#
# MapReduce concept test
# Based on https://www.wikiwand.com/en/MapReduce#/Examples
#

import concurrent.futures
import functools
import operator
import time
from pprint import pprint

# Text from Walden
text = """I went to the woods because I wished to live deliberately, to
front only the essential facts of life, and see if I could not learn what it had to teach, and
not, when I came to die, discover that I had not lived. I did not wish to live
what was not life, living is so dear; nor did I wish to practise resignation,
unless it was quite necessary. I wanted to live deep and suck out all the marrow
of life, to live so sturdily and Spartan like as to put to rout all that was not
life, to cut a broad swath and shave close, to drive life into a corner, and
reduce it to its lowest terms, and, if it proved to be mean, why then to get the
whole and genuine meanness of it, and publish its meanness to the world; or if
it were sublime, to know it by experience, and be able to give a true account of
it in my next excursion. For most men, it appears to me, are in a strange
uncertainty about it, whether it is of the devil or of God, and have somewhat
hastily concluded that it is the chief end of man here to glorify God and enjoy
him forever. Still we live meanly, like ants; though the fable tells us that we
were long ago changed into men; like pygmies we fight with cranes; it is error
upon error, and clout upon clout, and our best virtue has for its occasion a
superfluous and evitable wretchedness. Our life is frittered away by detail. An
honest man has hardly need to count more than his ten fingers, or in extreme
cases he may add his ten toes, and lump the rest. Simplicity, simplicity,
simplicity! I say, let your affairs be as two or three, and not a hundred or a
thousand; instead of a million count half a dozen, and keep your accounts on
your thumbnail. In the midst of this chopping sea of civilized life, such are
the clouds and storms and quicksands and thousand and one items to be allowed
for, that a man has to live, if he would not founder and go to the bottom and
not make his port at all, by dead reckoning, and he must be a great calculator
indeed who succeeds. Simplify, simplify."""

print(text)
print()

#
# 1st step: Map
# Divide work into smaller independent pieces. Do the necessary work on them.
#

lines = text.split('\n')

pprint(lines)
print()


def count_words(line):
    """Counts words in a given line
    """
    print(f'Counting words for pieces of length {len(line)}')
    time.sleep(1)
    return len(line.split())


# Start tracking time until task completion
start = time.time()

with concurrent.futures.ProcessPoolExecutor() as executor:
    map_result = tuple(executor.map(count_words, lines))

#
# 2nd step: Reduce
# Combine partial results as output result
#

# Could be represented as aka sum(map_result)
reduce_result = functools.reduce(operator.add, map_result, 0)

# Track the time after the whole operation took place
finish = time.time()

print(f'\nTime for map + reduce: {finish - start:.2f}s\n')
print(f'Result of the "map" step (words each line): {map_result}')
print(f'Result of the "reduce" step (total word count): {reduce_result}')
