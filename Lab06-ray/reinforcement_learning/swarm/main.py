from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.animation import FuncAnimation
from matplotlib.collections import Collection
from numpy.random import default_rng


FPS = 60


class Swarm:
    def __init__(
        self,
        width: float = 1000,
        height: float = 1000,
        near_threshold: float = 100,
        v0: Optional[float] = None,
        n: int = 140,
        aggregate: bool = True,
        seed: int = 0,
    ) -> None:
        self.width, self.height, self.near_threshold, self.n, self.aggregate = (
            width, height, near_threshold, n, aggregate,
        )
        self.v0 = v0 or (20 if aggregate else 2)
        self.rand = default_rng(seed=seed).random
        self.pos = width*self.rand((2, n))
        self.n_near_mean: float
        self.size = np.array((width, height))[:, np.newaxis]

    def get_v(self) -> tuple[
       np.ndarray,  # new speeds
       float,       # new mean proximate-neighbour count
    ]:
        displacement = self.pos[:, :, np.newaxis] - self.pos[:, np.newaxis, :]
        distance = np.linalg.norm(displacement, axis=0)
        n_near = np.count_nonzero(distance < self.near_threshold, axis=0)
        if self.aggregate:
            v = self.v0 / n_near
        else:
            v = self.v0 * n_near
        return v, n_near.mean()

    def update(self) -> None:
        v, self.n_near_mean = self.get_v()
        theta = 2*np.pi*self.rand(self.n)
        projections = np.stack((
            np.cos(theta), np.sin(theta),
        ))
        self.pos = np.mod(self.pos + v*projections, self.size)


def display(swarm: Swarm) -> tuple[plt.Figure, FuncAnimation]:
    fig: plt.Figure
    ax_bot: plt.Axes
    ax_stats: plt.Axes
    fig, (ax_bot, ax_stats) = plt.subplots(nrows=1, ncols=2)

    bot_scatter: Collection = ax_bot.scatter(*swarm.pos)
    ax_bot.set_title('Robot positions')
    ax_bot.set_xlabel('x')
    ax_bot.set_ylabel('y')
    ax_bot.axis((0, swarm.width, 0, swarm.height))

    frames = []
    means = []
    stat_line: plt.Line2D
    stat_line, = ax_stats.plot(frames, means)
    ax_stats.set_title('Mean proximate neighbours')
    ax_stats.set_xlabel('Timestep')
    ax_stats.set_ylabel('Count')
    ax_stats.axis((0, 15_000, 0, 14))

    def init() -> tuple[plt.Artist, ...]:
        return bot_scatter, stat_line

    def update(frame: int) -> tuple[plt.Artist, ...]:
        swarm.update()
        bot_scatter.set_offsets(swarm.pos.T)
        if frame % FPS:
            return bot_scatter,

        print(f'{swarm.n_near_mean:.1f}')
        frames.append(frame)
        means.append(swarm.n_near_mean)
        stat_line.set_data(frames, means)
        return bot_scatter, stat_line

    anim = FuncAnimation(
        fig=fig, func=update, init_func=init, interval=1e3/FPS, blit=True,
    )
    return fig, anim


def main() -> None:
    swarm = Swarm()
    print('Mean proximate neighbour count:')
    fig, anim = display(swarm)
    plt.show()


if __name__ == "__main__":
    main()