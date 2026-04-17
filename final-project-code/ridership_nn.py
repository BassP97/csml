import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from torch.optim.lr_scheduler import ReduceLROnPlateau

with open("light_rail_data/stop_ridership.csv", "r", encoding="utf-8") as f:
    df = pd.read_csv(f)


df = df.replace(-666666666, np.nan)
median = df.groupby("agency")["avg_boardings_per_day"].transform("median")
df["above_median"] = (df["avg_boardings_per_day"] > median).astype(np.float32)


big_features = [
    "population",
    "median_household_income",
    "per_capita_income",
    "median_home_value",
    "median_rent",
]

percentages = [
    "percent_under_5",
    "pct_over_85",
    "pct_bachelors_or_higher",
    "pct_labor_force_unemployed",
    "pct_married",
    "pct_with_health_insurance",
    "pct_foreign_born",
    "pct_renter_occupied",
    "pct_with_computer",
    "pct_with_internet",
    "pct_no_vehicle_available",
    "pct_commute_by_transit",
    "pct_work_from_home",
    "pct_multi_unit_housing",
]

for col in big_features:
    df[col] = df.groupby("agency")[col].rank(pct=True)

for col in percentages:
    df[col] = df[col] / 100.0
print(df[big_features])
print(df[percentages])
agency_dummies = pd.get_dummies(df["agency"], prefix="agency", dtype=float)
df = pd.concat([df, agency_dummies], axis=1)

columns = big_features + percentages + list(agency_dummies.columns)

X = df[columns].values
X = np.where(np.isnan(X), np.nanmedian(X, axis=0), X)
y = df["above_median"].values

agencies = df["agency"].values
idx = np.arange(len(df))

train_idx, val_idx, test_idx = [], [], []
for agency in np.unique(agencies):
    agency_mask = agencies == agency
    agency_idx = idx[agency_mask]
    np.random.shuffle(agency_idx)

    n = len(agency_idx)
    n_train = int(0.75 * n)
    n_val = int(0.125 * n)

    train_idx.extend(agency_idx[:n_train])
    val_idx.extend(agency_idx[n_train : n_train + n_val])
    test_idx.extend(agency_idx[n_train + n_val :])

train = np.array(train_idx)
val = np.array(val_idx)
test = np.array(test_idx)


class loader:
    def __init__(self, idx):
        self.dataset = TensorDataset(
            torch.tensor(X[idx], dtype=torch.float32),
            torch.tensor(y[idx], dtype=torch.float32).unsqueeze(-1),
        )

    def __iter__(self):
        return iter(DataLoader(self.dataset, batch_size=64, shuffle=True))


train_loader = loader(train)
val_loader = loader(val)
test_loader = loader(test)


class RidershipClassifier(nn.Module):
    def __init__(self, n_features: int):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_features, 32),
            nn.BatchNorm1d(32),
            nn.ReLU(),
            # nn.Dropout(0.1),
            nn.Linear(32, 16),
            nn.BatchNorm1d(16),
            nn.ReLU(),
            # nn.Dropout(0.1),
            nn.Linear(16, 1),
        )

    def forward(self, x):
        return self.net(x)


model = RidershipClassifier(n_features=X.shape[1])
optimizer = torch.optim.Adam(model.parameters())
scheduler = ReduceLROnPlateau(optimizer)
loss = nn.BCEWithLogitsLoss()

best_val_loss = float("inf")
best_state = None
patience_counter = 0

for epoch in range(1, 300):
    model.train()
    total_loss = 0.0
    for Xb, yb in train_loader:
        optimizer.zero_grad()
        loss_value = loss(model(Xb), yb)
        loss_value.backward()
        optimizer.step()
        total_loss += loss_value.item() * len(Xb)
    train_loss = total_loss / len(train_idx)

    model.eval()
    total_val = 0.0
    with torch.no_grad():
        for Xb, yb in val_loader:
            total_val += loss(model(Xb), yb).item() * len(Xb)
    val_loss = total_val / len(val_idx)

    scheduler.step(val_loss)
    print(
        "Epoch %3d | train loss: %.4f | val loss: %.4f" % (epoch, train_loss, val_loss)
    )
    if val_loss < best_val_loss:
        best_val_loss = val_loss
        best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        patience_counter = 0
    else:
        patience_counter += 1

    if epoch % 25 == 0 or epoch == 1:
        print(
            f"Epoch {epoch:3d} | train loss: {train_loss:.4f} | val loss: {val_loss:.4f}"
        )

    if patience_counter >= 20:
        break

#
model.load_state_dict(best_state)
model.eval()


def evaluate(loader, idx, split_name):
    logits, targets = [], []
    with torch.no_grad():
        for Xb, yb in loader:
            logits.append(model(Xb).cpu().squeeze(-1))
            targets.append(yb.squeeze(-1))
    logits = torch.cat(logits).numpy()
    targets = torch.cat(targets).numpy()

    probs = torch.sigmoid(torch.tensor(logits)).numpy()
    preds = (probs >= 0.5).astype(float)

    acc = accuracy_score(targets, preds)
    f1 = f1_score(targets, preds, zero_division=0)
    auc = roc_auc_score(targets, probs)

    print(
        f"{split_name} set: n={len(idx):3d}  Acc={acc:.4f}  F1={f1:.4f}  AUC={auc:.4f}"
    )
    return preds, probs, targets


evaluate(train_loader, train_idx, "Train")
evaluate(val_loader, val_idx, "Validation")
test_preds, test_probs, test_targets = evaluate(test_loader, test_idx, "Test")
print("accuracy by agency:")
for agency in np.unique(agencies):
    agency_mask = agencies[test_idx] == agency
    if np.sum(agency_mask) > 0:
        acc = accuracy_score(test_targets[agency_mask], test_preds[agency_mask])
        print(f"  {agency}: {acc:.4f}")
