"""Prophet model training for occupancy prediction."""
import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Dict, Any, Tuple

# Default hyperparameters from tuned notebook
PROPHET_DEFAULTS = {
    "changepoint_prior_scale": 0.002,
    "seasonality_prior_scale": 25,
    "changepoint_range": 0.85,
    "daily_seasonality": True,
    "weekly_seasonality": True,
    "yearly_seasonality": True,
    # Quarterly seasonality
    "q1_period": 70,
    "q1_fourier_order": 30,
    "q1_prior_scale": 1,
    "q2_period": 84,
    "q2_fourier_order": 21,
    "q2_prior_scale": 320,
    "q3_period": 90,
    "q3_fourier_order": 5,
    "q3_prior_scale": 1,
    "q4_period": 60,
    "q4_fourier_order": 15,
    "q4_prior_scale": 1,
    # Weekend pattern
    "weekend_period": 7,
    "weekend_fourier_order": 30,
    "weekend_prior_scale": 50,
    # Active hours pattern
    "active_period": 1,
    "active_fourier_order": 22,
    "active_prior_scale": 640,
}


def get_holidays() -> pd.DataFrame:
    """Get UCI academic calendar holidays."""
    holiday_periods = [
        # Fall 2017
        ("Veterans Day", "2017-11-10", "2017-11-10"),
        ("Thanksgiving", "2017-11-23", "2017-11-24"),
        ("Winter Break", "2017-12-18", "2018-01-02"),
        ("Winter Admin Recess", "2017-12-25", "2018-01-02"),
        # Winter 2018
        ("MLK Day", "2018-01-15", "2018-01-15"),
        ("Presidents Day", "2018-02-19", "2018-02-19"),
        ("Cesar Chavez Day", "2018-03-30", "2018-03-30"),
        # Spring 2018
        ("Memorial Day", "2018-05-28", "2018-05-28"),
        ("Spring Break", "2018-03-26", "2018-03-30"),
    ]

    dfs = []
    for name, start, end in holiday_periods:
        dates = pd.date_range(start=start, end=end, freq="D")
        df = pd.DataFrame({"ds": dates, "holiday": name})
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add quarter, day, hour, and binary features for Prophet seasonality."""
    df = df.copy()
    dates = pd.to_datetime(df['ds'])
    months = dates.dt.month
    days = dates.dt.day

    # Create month_day for quarter calculation (UCI academic quarters)
    month_day = months * 100 + days

    # Define quarters based on UCI academic calendar
    q1_mask = (month_day >= 924) & (month_day <= 1214)  # Sep 24 - Dec 14
    q2_mask = (month_day >= 102) & (month_day <= 322)   # Jan 2 - Mar 22
    q3_mask = (month_day >= 327) & (month_day <= 614)   # Mar 27 - Jun 14
    q4_mask = (month_day >= 624) & (month_day <= 830)   # Jun 24 - Aug 30

    quarter = np.select([q1_mask, q2_mask, q3_mask, q4_mask], [1, 2, 3, 4], default=0)

    # Add features
    df['quarter'] = quarter
    df['day'] = dates.dt.dayofweek
    df['hour'] = dates.dt.hour
    df['month'] = months

    # Binary features for conditional seasonality
    df['quarter_1'] = (df['quarter'] == 1).astype(int)
    df['quarter_2'] = (df['quarter'] == 2).astype(int)
    df['quarter_3'] = (df['quarter'] == 3).astype(int)
    df['quarter_4'] = (df['quarter'] == 4).astype(int)
    df['is_weekend'] = (dates.dt.dayofweek >= 5).astype(int)
    df['is_active_hour'] = (df['hour'].between(8, 22)).astype(int)

    return df


def create_timeline(df: pd.DataFrame, interval_seconds: int) -> pd.DataFrame:
    """
    Fill missing time intervals and handle inactive hours.

    Args:
        df: DataFrame with 'ds' and 'y' columns
        interval_seconds: Time bin size in seconds
    """
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

    # Forward fill remaining gaps (limit based on interval)
    # For 15-min intervals: limit=4 means 1 hour max fill
    fill_limit = max(1, 3600 // interval_seconds)
    result['y'] = result['y'].ffill(limit=fill_limit)

    # Drop remaining NaN rows
    result = result.dropna(subset=['y'])

    return result[['ds', 'y']].sort_values('ds').reset_index(drop=True)


def prepare_data_from_parquet(df: pd.DataFrame, interval_seconds: int) -> pd.DataFrame:
    """
    Prepare occupancy parquet data for Prophet training.

    Args:
        df: DataFrame with 'interval_begin_time' and 'number_connections' columns
        interval_seconds: Time bin size

    Returns:
        DataFrame ready for Prophet with 'ds', 'y' and feature columns
    """
    # Rename columns for Prophet
    df = df.rename(columns={
        'interval_begin_time': 'ds',
        'number_connections': 'y'
    })
    df['ds'] = pd.to_datetime(df['ds'])
    df = df.sort_values('ds').reset_index(drop=True)

    # Fill timeline gaps
    df = create_timeline(df, interval_seconds)

    # Add features
    df = add_features(df)

    return df


def train_prophet_model(
    df: pd.DataFrame,
    config: Dict[str, Any],
    test_size: float = 0.1
) -> Tuple[Prophet, MinMaxScaler, Dict[str, float]]:
    """
    Train a Prophet model on occupancy data.

    Args:
        df: Prepared DataFrame with 'ds', 'y' and feature columns
        config: Hyperparameters (merged with PROPHET_DEFAULTS)
        test_size: Fraction of data for testing

    Returns:
        (model, scaler, metrics) tuple
    """
    # Merge with defaults
    full_config = {**PROPHET_DEFAULTS, **config}

    # Train/test split
    split_idx = int(len(df) * (1 - test_size))
    train_df = df.iloc[:split_idx].copy()
    test_df = df.iloc[split_idx:].copy()

    # Scale target
    scaler = MinMaxScaler()
    train_df = train_df.dropna(subset=['y'])
    train_df['y'] = scaler.fit_transform(train_df[['y']])

    # Create model with holidays
    holidays = get_holidays()
    model = Prophet(
        holidays=holidays,
        changepoint_prior_scale=full_config["changepoint_prior_scale"],
        seasonality_prior_scale=full_config["seasonality_prior_scale"],
        changepoint_range=full_config["changepoint_range"],
        daily_seasonality=full_config["daily_seasonality"],
        weekly_seasonality=full_config["weekly_seasonality"],
        yearly_seasonality=full_config["yearly_seasonality"],
    )

    # Add quarterly seasonality patterns
    model.add_seasonality(
        name='quarterly_pattern1',
        period=full_config["q1_period"],
        fourier_order=full_config["q1_fourier_order"],
        prior_scale=full_config["q1_prior_scale"],
        condition_name='quarter_1'
    )
    model.add_seasonality(
        name='quarterly_pattern2',
        period=full_config["q2_period"],
        fourier_order=full_config["q2_fourier_order"],
        prior_scale=full_config["q2_prior_scale"],
        condition_name='quarter_2'
    )
    model.add_seasonality(
        name='quarterly_pattern3',
        period=full_config["q3_period"],
        fourier_order=full_config["q3_fourier_order"],
        prior_scale=full_config["q3_prior_scale"],
        condition_name='quarter_3'
    )
    model.add_seasonality(
        name='quarterly_pattern4',
        period=full_config["q4_period"],
        fourier_order=full_config["q4_fourier_order"],
        prior_scale=full_config["q4_prior_scale"],
        condition_name='quarter_4'
    )

    # Add weekend pattern
    model.add_seasonality(
        name='weekend_pattern',
        period=full_config["weekend_period"],
        fourier_order=full_config["weekend_fourier_order"],
        prior_scale=full_config["weekend_prior_scale"],
        condition_name='is_weekend'
    )

    # Add active hours pattern
    model.add_seasonality(
        name='active_pattern',
        period=full_config["active_period"],
        fourier_order=full_config["active_fourier_order"],
        prior_scale=full_config["active_prior_scale"],
        condition_name='is_active_hour'
    )

    # Fit model
    model.fit(train_df)

    # Evaluate on test set
    forecast = model.predict(test_df)
    forecast['yhat'] = forecast['yhat'].clip(lower=0)

    # Inverse transform predictions
    forecast['yhat'] = scaler.inverse_transform(forecast[['yhat']])
    test_actual = scaler.inverse_transform(test_df[['y']].values)

    # Calculate metrics
    rmse = float(np.sqrt(mean_squared_error(test_actual, forecast['yhat'].values)))
    mae = float(mean_absolute_error(test_actual, forecast['yhat'].values))

    metrics = {
        "rmse": rmse,
        "mae": mae,
        "train_size": len(train_df),
        "test_size": len(test_df),
    }

    return model, scaler, metrics
