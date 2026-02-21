"""Transformer model training for occupancy prediction."""
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from torch.optim.lr_scheduler import ReduceLROnPlateau
from typing import Dict, Any, Tuple

# Default hyperparameters from tuned notebook
TRANSFORMER_DEFAULTS = {
    "d_model": 64,
    "nhead": 4,
    "num_layers": 2,
    "dropout": 0.2,
    "sequence_size": 96,  # 24 hours of 15-min intervals
    "learning_rate": 0.001,
    "batch_size": 32,
    "max_epochs": 100,
    "patience": 5,
}


class PositionalEncoding(nn.Module):
    """Positional encoding for transformer input."""

    def __init__(self, d_model: int, dropout: float = 0.1, max_len: int = 5000):
        super(PositionalEncoding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, d_model, 2).float() * (-np.log(10000.0) / d_model)
        )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0).transpose(0, 1)
        self.register_buffer('pe', pe)

    def forward(self, x):
        x = x + self.pe[:x.size(0), :]
        return self.dropout(x)


class TransformerModel(nn.Module):
    """Transformer encoder for time series prediction."""

    def __init__(
        self,
        input_dim: int = 6,
        d_model: int = 64,
        nhead: int = 4,
        num_layers: int = 2,
        dropout: float = 0.2
    ):
        super(TransformerModel, self).__init__()
        self.encoder = nn.Linear(input_dim, d_model)
        self.pos_encoder = PositionalEncoding(d_model, dropout)
        encoder_layers = nn.TransformerEncoderLayer(d_model, nhead, batch_first=True)
        self.transformer_encoder = nn.TransformerEncoder(encoder_layers, num_layers)
        self.decoder = nn.Linear(d_model, 1)

    def forward(self, x):
        x = self.encoder(x)
        x = self.pos_encoder(x)
        x = self.transformer_encoder(x)
        x = self.decoder(x[:, -1, :])  # Use last timestep for prediction
        return x


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add cyclical time features for transformer input."""
    df = df.copy()
    df["hour"] = df["ds"].dt.hour + df["ds"].dt.minute / 60
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df["ds"].dt.dayofweek / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["ds"].dt.dayofweek / 7)
    df["holiday"] = 0  # Simplified - could add holiday detection
    return df


def create_timeline(df: pd.DataFrame, interval_seconds: int) -> pd.DataFrame:
    """Fill missing time intervals."""
    freq = f"{interval_seconds}s"
    start_date = df['ds'].min()
    end_date = df['ds'].max()

    # Create complete timeline
    full_timeline = pd.date_range(start=start_date, end=end_date, freq=freq)
    complete_df = pd.DataFrame({'ds': full_timeline})
    result = complete_df.merge(df[['ds', 'y']], on='ds', how='left')

    # Fill inactive hours (10 PM - 8 AM) with 0
    result['hour'] = result['ds'].dt.hour
    inactive_hours = list(range(22, 24)) + list(range(0, 8))
    inactive_mask = result['hour'].isin(inactive_hours)
    result.loc[inactive_mask & result['y'].isna(), 'y'] = 0

    # Fill remaining NaN with 0
    result['y'] = result['y'].fillna(0)

    return result[['ds', 'y']].sort_values('ds').reset_index(drop=True)


def prepare_data_from_parquet(df: pd.DataFrame, interval_seconds: int) -> pd.DataFrame:
    """
    Prepare occupancy parquet data for Transformer training.

    Args:
        df: DataFrame with 'interval_begin_time' and 'number_connections' columns
        interval_seconds: Time bin size

    Returns:
        DataFrame ready for training with 'ds', 'y' and feature columns
    """
    # Rename columns
    df = df.rename(columns={
        'interval_begin_time': 'ds',
        'number_connections': 'y'
    })
    df['ds'] = pd.to_datetime(df['ds'])
    df['y'] = df['y'].astype(float)
    df = df.sort_values('ds').reset_index(drop=True)

    # Fill timeline gaps
    df = create_timeline(df, interval_seconds)

    # Add features
    df = add_features(df)

    return df


def create_sequences(
    data: np.ndarray,
    sequence_size: int,
    start_time: pd.Timestamp
) -> Tuple[torch.Tensor, torch.Tensor, list]:
    """
    Create sliding window sequences for training.

    Args:
        data: Array of shape (n_samples, n_features)
        sequence_size: Number of timesteps to look back
        start_time: Start timestamp for tracking

    Returns:
        (x, y, timestamps) tuple
    """
    x, y, times = [], [], []
    for i in range(len(data) - sequence_size):
        window = data[i:(i + sequence_size)]
        target = data[i + sequence_size, 0]  # y is first column
        x.append(window)
        y.append(target)
        times.append(start_time + pd.Timedelta(seconds=15 * 60 * (sequence_size + i)))

    x = np.array(x)
    y = np.array(y)
    return (
        torch.tensor(x, dtype=torch.float32),
        torch.tensor(y, dtype=torch.float32).view(-1, 1),
        times
    )


def get_device() -> torch.device:
    """Auto-detect best available device (GPU if available, else CPU)."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")  # Apple Silicon
    return torch.device("cpu")


def train_transformer_model(
    df: pd.DataFrame,
    config: Dict[str, Any],
    test_size: float = 0.1,
    interval_seconds: int = 900
) -> Tuple[TransformerModel, StandardScaler, Dict[str, float]]:
    """
    Train a Transformer model on occupancy data.

    Args:
        df: Prepared DataFrame with 'ds', 'y' and feature columns
        config: Hyperparameters (merged with TRANSFORMER_DEFAULTS)
        test_size: Fraction of data for testing
        interval_seconds: Time interval for sequence adjustment

    Returns:
        (model, scaler, metrics) tuple
    """
    # Auto-detect device (GPU/CPU)
    device = get_device()
    print(f"Training on device: {device}")

    # Merge with defaults
    full_config = {**TRANSFORMER_DEFAULTS, **config}

    # Adjust sequence size based on interval (target 24 hours lookback)
    intervals_per_day = 86400 // interval_seconds
    sequence_size = full_config.get("sequence_size", intervals_per_day)

    # Train/test split
    split_idx = int(len(df) * (1 - test_size))
    df_train = df.iloc[:split_idx].copy()
    df_test = df.iloc[split_idx:].copy()

    train_start = df_train['ds'].min()
    test_start = df_test['ds'].min()

    # Feature columns
    feature_cols = ["y", "hour_sin", "hour_cos", "dow_sin", "dow_cos", "holiday"]

    # Scale target
    scaler = StandardScaler()
    train_y = scaler.fit_transform(df_train[["y"]])
    test_y = scaler.transform(df_test[["y"]])

    # Prepare feature arrays
    train_features = df_train[["hour_sin", "hour_cos", "dow_sin", "dow_cos", "holiday"]].to_numpy()
    test_features = df_test[["hour_sin", "hour_cos", "dow_sin", "dow_cos", "holiday"]].to_numpy()

    # Combine y with features
    train_data = np.hstack([train_y, train_features])
    test_data = np.hstack([test_y, test_features])

    # Create sequences
    x_train, y_train, _ = create_sequences(train_data, sequence_size, train_start)
    x_test, y_test, test_times = create_sequences(test_data, sequence_size, test_start)

    if len(x_train) == 0 or len(x_test) == 0:
        raise ValueError(
            f"Not enough data for sequence_size={sequence_size}. "
            f"Train: {len(df_train)}, Test: {len(df_test)}"
        )

    # Move data to device
    x_train = x_train.to(device)
    y_train = y_train.to(device)
    x_test = x_test.to(device)
    y_test = y_test.to(device)

    # Data loaders
    batch_size = full_config["batch_size"]
    train_dataset = TensorDataset(x_train, y_train)
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_dataset = TensorDataset(x_test, y_test)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    # Create model and move to device
    input_dim = x_train.shape[2]
    model = TransformerModel(
        input_dim=input_dim,
        d_model=full_config["d_model"],
        nhead=full_config["nhead"],
        num_layers=full_config["num_layers"],
        dropout=full_config["dropout"]
    ).to(device)

    # Training setup
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=full_config["learning_rate"])
    scheduler = ReduceLROnPlateau(optimizer, 'min', factor=0.5, patience=3)

    max_epochs = full_config["max_epochs"]
    patience = full_config["patience"]
    early_stop_count = 0
    min_val_loss = float('inf')
    best_model_state = None

    # Training loop
    for epoch in range(max_epochs):
        model.train()
        for batch in train_loader:
            x_batch, y_batch = batch  # Already on device
            optimizer.zero_grad()
            outputs = model(x_batch)
            loss = criterion(outputs, y_batch)
            loss.backward()
            optimizer.step()

        # Validation
        model.eval()
        val_losses = []
        with torch.no_grad():
            for batch in test_loader:
                x_batch, y_batch = batch  # Already on device
                outputs = model(x_batch)
                loss = criterion(outputs, y_batch)
                val_losses.append(loss.item())

        val_loss = np.mean(val_losses)
        scheduler.step(val_loss)

        if val_loss < min_val_loss:
            min_val_loss = val_loss
            early_stop_count = 0
            best_model_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            early_stop_count += 1

        if early_stop_count >= patience:
            print(f"Early stopping at epoch {epoch + 1}")
            break

    # Load best model (on CPU for saving)
    model_cpu = TransformerModel(
        input_dim=input_dim,
        d_model=full_config["d_model"],
        nhead=full_config["nhead"],
        num_layers=full_config["num_layers"],
        dropout=full_config["dropout"]
    )
    if best_model_state is not None:
        model_cpu.load_state_dict(best_model_state)

    # Final evaluation (use GPU model for speed)
    if best_model_state is not None:
        model.load_state_dict({k: v.to(device) for k, v in best_model_state.items()})

    model.eval()
    predictions = []
    with torch.no_grad():
        for batch in test_loader:
            x_batch, _ = batch
            outputs = model(x_batch)
            out = outputs.squeeze().cpu()  # Move to CPU for numpy
            if out.ndim == 0:
                predictions.append(out.item())
            else:
                predictions.extend(out.tolist())

    # Inverse transform
    predictions_unscaled = scaler.inverse_transform(
        np.array(predictions).reshape(-1, 1)
    ).flatten()
    actuals_unscaled = scaler.inverse_transform(
        y_test.cpu().numpy().reshape(-1, 1)
    ).flatten()

    # Calculate metrics
    rmse = float(np.sqrt(mean_squared_error(actuals_unscaled, predictions_unscaled)))
    mae = float(mean_absolute_error(actuals_unscaled, predictions_unscaled))

    metrics = {
        "rmse": rmse,
        "mae": mae,
        "train_size": len(x_train),
        "test_size": len(x_test),
        "final_val_loss": float(min_val_loss),
        "device": str(device),
    }

    return model_cpu, scaler, metrics
