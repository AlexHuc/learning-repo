import numpy as np

aggregate_shorts_made = []
for i in range(0, 10000):
    shots_made = []
    for k in range(0, 100):
        r = np.random.random()
        if r < 0.7:
            shots_made.append(1)
        else:
            shots_made.append(0)
    if sum(shots_made) >= 70:
        aggregate_shorts_made.append(1)
    else:
        aggregate_shorts_made.append(0)

    print(np.average(aggregate_shorts_made))