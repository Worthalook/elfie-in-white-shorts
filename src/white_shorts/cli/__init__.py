from __future__ import annotations
import typer
from .train import app as train_app
from .predict import app as predict_app
from .log_actuals import app as actuals_app
from .dashboards import app as dashboards_app

app = typer.Typer(help="WhiteShorts 3.0 CLI")
app.add_typer(train_app, name="train")
app.add_typer(predict_app, name="predict")
app.add_typer(actuals_app, name="log-actuals")
app.add_typer(dashboards_app, name="dashboards")
