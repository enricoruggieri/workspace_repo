# region-specific means (one per region)
mu_region = np.random.uniform(-1, 1, R)  # shape (R,)

# online indicator for each degree: 1 = online, 0 = in-person.
online_indic = np.random.choice([0, 1], size=J, p=[0.8, 0.2]) ## 80% in person 20% online

# parameters for simulation
psi = 0.3    # interaction term
delta = 0.2  # factor for age bins (only if degree is online)

# initialize W matrix of shape (J, R*A)
W = np.zeros((J, R * A))

# for online random draw gets boost increasing with age bin.
for j in range(J):
    for r in range(R):
        for a in range(A):
            index = r * A + a  # flatten (r, a) into one index
            if online_flag[j] == 1:
                # online degrees: boost that increases with age bin (using a+1 so that bin 0 gets delta)
                W[j, index] = np.random.randn() + delta * (a + 1) - 1
            else:
                # in-person degrees: standard random draw with positive shift
                W[j, index] = np.random.randn() + 1

# mean vector for each degree.
#  mu[j, index] = mu_region[r] + psi * W[j, index]
mu = np.zeros((J, R * A))
for j in range(J):
    for r in range(R):
        for a in range(A):
            index = r * A + a
            mu[j, index] = mu_region[r] + psi * W[j, index]

# define covariance matrix Sigma for (region, age) effects.
Sigma_dim = R * A  # 20 * 4 = 80
Sigma = np.eye(Sigma_dim) * 0.5 + np.random.rand(Sigma_dim, Sigma_dim) * 0.1
Sigma = (Sigma + Sigma.T) / 2  # ensure symmetry

# draw multivariate normal sample with mean vector mu[j] and covariance Sigma.
# VA_samples will be an array of shape (J, Sigma_dim)
VA_samples = np.array([np.random.multivariate_normal(mu[j], Sigma) for j in range(J)])

# aggregate degree-specific VA by taking the average across all (region, age) cells.
degree_value_added = np.mean(VA_samples, axis=1)

# create a DataFrame for degrees with overall VA and online indic.
degrees_df = pd.DataFrame({
    "assigned_degree": np.arange(J),  
    "degree_value_added": degree_value_added,
    "online": online_indic
})

# shift degree IDs so that degrees run from 1 to J (0 reserved for no degree)
degrees_df["assigned_degree"] = degrees_df["assigned_degree"] + 1
