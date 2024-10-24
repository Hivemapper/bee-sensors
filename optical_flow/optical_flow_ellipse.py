import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse

def create_ellipse(weights, output_path, metric):
    # Get the x and y coordinates of each pixel
    weights = np.where(weights > 0.1*np.max(weights), weights, 0)
    
    y_indices, x_indices = np.indices(weights.shape)

    # Flatten the indices and the weights
    x_flat = x_indices.flatten()
    y_flat = y_indices.flatten()
    weights_flat = weights.flatten()

    # Compute the weighted mean
    mean_x = np.average(x_flat, weights=weights_flat)
    mean_y = np.average(y_flat, weights=weights_flat)
    mean = np.array([mean_x, mean_y])

    # Compute the weighted covariance matrix
    cov_matrix = np.cov(np.vstack((x_flat, y_flat)), aweights=weights_flat)

    # Function to plot the covariance ellipse
    def plot_covariance_ellipse(mean, cov, ax=None, n_std=3.0, facecolor='none', **kwargs):
        if ax is None:
            ax = plt.gca()

        # Eigenvalues and eigenvectors of the covariance matrix
        eigvals, eigvecs = np.linalg.eigh(cov)
        
        # Sort eigenvalues and eigenvectors
        order = eigvals.argsort()[::-1]
        eigvals, eigvecs = eigvals[order], eigvecs[:, order]
        
        # Calculate the angle of the ellipse
        angle = np.degrees(np.arctan2(*eigvecs[:, 0][::-1]))

        # Width and height of the ellipse (based on n_std deviations)
        width, height = 2 * n_std * np.sqrt(eigvals)

        print("eignvalues",np.sqrt(eigvals),np.linalg.norm(eigvals))

        # Create the ellipse patch
        ellipse = Ellipse(mean, width, height, angle, facecolor=facecolor, **kwargs)
        

        ax.add_patch(ellipse)

        return eigvals

    # Plot the data (weights image), the weighted mean, and the covariance ellipse
    fig, ax = plt.subplots()

    # Show the weights as an image
    im = ax.imshow(weights, origin='lower')

    # Plot the weighted mean
    ax.scatter(*mean, color='red', marker='x', label='Weighted Mean')

    # Plot the covariance ellipse
    eigvals = plot_covariance_ellipse(mean, cov_matrix, ax, edgecolor='red')

    ax.set_title('Norm'+str(np.round(np.linalg.norm(eigvals),2)) \
                 + ' Max'+str(np.round(np.max(eigvals),2)) \
                 + ' Sum'+str(np.round(np.sum(eigvals),2))
                )
    ax.legend()
    plt.colorbar(im, ax=ax)

    grid_save_path = os.path.join(output_path,"grid"+str(time.time())+".png")
    fig.savefig(grid_save_path)
    plt.close(fig)