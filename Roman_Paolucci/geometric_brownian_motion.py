import matplotlib.pyplot as plt
import numpy as np
import math

class StochasticProcess:

    def time_step(self):
        dW = np.random.normal(0, math.sqrt(self.delta_t))
        dS = self.drift * self.delta_t * self.current_asset_price + self.volatility * self.current_asset_price * dW
        self.asset_prices.append(self.current_asset_price + dS)
        self.current_asset_price += dS

    def __init__(self, drift, volatility, delta_t, initial_asset_price):
        self.drift = drift
        self.volatility = volatility
        self.delta_t = delta_t
        self.current_asset_price = initial_asset_price
        self.asset_prices = [initial_asset_price]

processes = []
for i in range(0, 100):
    processes.append(StochasticProcess(drift=0.2, volatility=0.3, delta_t=1/365, initial_asset_price=300))

for process in processes:
    tte = 1
    while(tte - process.delta_t > 0):
        process.time_step()
        tte -= process.delta_t

print(processes[0].asset_prices)

x = plt.plot(np.arange(0, len(processes[0].asset_prices)), processes[0].asset_prices)
plt.show()