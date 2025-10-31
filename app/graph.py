import matplotlib.pyplot as plt
import pandas as pd

# The provided data sample, replace this with your actual data in the same format
data = [
  {
    "user": "cyclist",
    "severities": [
      {
        "severity": "severe",
        "event_types": [
          {
            "event_type": "brake",
            "deciles": [
              { "D": 9, "count": 75 },
              { "D": 2, "count": 1 },
              { "D": 5, "count": 10 },
              { "D": 7, "count": 31 },
              { "D": 1, "count": 1 },
              { "D": 8, "count": 61 },
              { "D": 6, "count": 18 },
              { "D": 4, "count": 5 }
            ],
            "total": 202
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 7, "count": 35 },
              { "D": 5, "count": 8 },
              { "D": 9, "count": 64 },
              { "D": 6, "count": 14 },
              { "D": 8, "count": 49 },
              { "D": 3, "count": 8 },
              { "D": 4, "count": 14 },
              { "D": 2, "count": 7 }
            ],
            "total": 199
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 6, "count": 20 },
              { "D": 5, "count": 4 },
              { "D": 8, "count": 42 },
              { "D": 7, "count": 38 },
              { "D": 4, "count": 13 },
              { "D": 2, "count": 11 },
              { "D": 9, "count": 65 }
            ],
            "total": 193
          },
          {
            "event_type": "speedup",
            "deciles": [
              { "D": 7, "count": 21 },
              { "D": 9, "count": 8 }
            ],
            "total": 29
          }
        ]
      },
      {
        "severity": "mass",
        "event_types": [
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 8, "count": 2 },
              { "D": 9, "count": 2 }
            ],
            "total": 4
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 8, "count": 2 },
              { "D": 9, "count": 2 }
            ],
            "total": 4
          },
          {
            "event_type": "brake",
            "deciles": [{ "D": 9, "count": 4 }],
            "total": 4
          },
          {
            "event_type": "speedup",
            "deciles": [{ "D": 7, "count": 1 }],
            "total": 1
          }
        ]
      },
      {
        "severity": "light",
        "event_types": [
          {
            "event_type": "cornering_right",
            "deciles": [{ "D": 9, "count": 6 }],
            "total": 6
          },
          {
            "event_type": "brake",
            "deciles": [
              { "D": 9, "count": 5 },
              { "D": 7, "count": 1 }
            ],
            "total": 6
          },
          {
            "event_type": "speedup",
            "deciles": [
              { "D": 7, "count": 1 },
              { "D": 9, "count": 1 }
            ],
            "total": 2
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 9, "count": 5 },
              { "D": 8, "count": 1 }
            ],
            "total": 6
          }
        ]
      }
    ]
  },
  {
    "user": "motorcycle",
    "severities": [
      {
        "severity": "severe",
        "event_types": [
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 6, "count": 119 },
              { "D": 2, "count": 49 },
              { "D": 3, "count": 43 },
              { "D": 4, "count": 69 },
              { "D": 5, "count": 74 },
              { "D": 7, "count": 189 },
              { "D": 8, "count": 320 },
              { "D": 9, "count": 470 },
              { "D": 10, "count": 1 }
            ],
            "total": 1334
          },
          {
            "event_type": "speedup",
            "deciles": [
              { "D": 9, "count": 34 },
              { "D": 7, "count": 122 }
            ],
            "total": 156
          },
          {
            "event_type": "brake",
            "deciles": [
              { "D": 4, "count": 28 },
              { "D": 9, "count": 633 },
              { "D": 3, "count": 6 },
              { "D": 5, "count": 55 },
              { "D": 7, "count": 164 },
              { "D": 1, "count": 5 },
              { "D": 2, "count": 3 },
              { "D": 8, "count": 372 },
              { "D": 6, "count": 104 }
            ],
            "total": 1370
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 2, "count": 75 },
              { "D": 5, "count": 64 },
              { "D": 6, "count": 105 },
              { "D": 4, "count": 95 },
              { "D": 7, "count": 201 },
              { "D": 8, "count": 290 },
              { "D": 9, "count": 463 }
            ],
            "total": 1293
          }
        ]
      },
      {
        "severity": "light",
        "event_types": [
          {
            "event_type": "brake",
            "deciles": [
              { "D": 1, "count": 1 },
              { "D": 9, "count": 135 },
              { "D": 8, "count": 10 },
              { "D": 7, "count": 1 }
            ],
            "total": 147
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 6, "count": 3 },
              { "D": 8, "count": 22 },
              { "D": 9, "count": 105 },
              { "D": 2, "count": 3 },
              { "D": 4, "count": 1 },
              { "D": 7, "count": 10 }
            ],
            "total": 144
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 9, "count": 99 },
              { "D": 3, "count": 2 },
              { "D": 7, "count": 7 },
              { "D": 5, "count": 4 },
              { "D": 8, "count": 30 },
              { "D": 6, "count": 5 }
            ],
            "total": 147
          },
          {
            "event_type": "speedup",
            "deciles": [
              { "D": 9, "count": 11 },
              { "D": 7, "count": 34 }
            ],
            "total": 45
          }
        ]
      },
      {
        "severity": "mass",
        "event_types": [
          {
            "event_type": "speedup",
            "deciles": [{ "D": 7, "count": 3 }],
            "total": 3
          },
          {
            "event_type": "brake",
            "deciles": [{ "D": 9, "count": 8 }],
            "total": 8
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 8, "count": 4 },
              { "D": 9, "count": 4 }
            ],
            "total": 8
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 8, "count": 1 },
              { "D": 4, "count": 1 },
              { "D": 6, "count": 1 },
              { "D": 9, "count": 5 }
            ],
            "total": 8
          }
        ]
      }
    ]
  },
  {
    "user": "general",
    "severities": [
      {
        "severity": "severe",
        "event_types": [
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 9, "count": 1122 },
              { "D": 6, "count": 212 },
              { "D": 8, "count": 617 },
              { "D": 5, "count": 143 },
              { "D": 4, "count": 121 },
              { "D": 3, "count": 82 },
              { "D": 7, "count": 364 },
              { "D": 2, "count": 87 },
              { "D": 10, "count": 1 }
            ],
            "total": 2749
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 9, "count": 1096 },
              { "D": 2, "count": 147 },
              { "D": 5, "count": 111 },
              { "D": 8, "count": 577 },
              { "D": 4, "count": 188 },
              { "D": 6, "count": 191 },
              { "D": 10, "count": 1 },
              { "D": 7, "count": 379 }
            ],
            "total": 2690
          },
          {
            "event_type": "brake",
            "deciles": [
              { "D": 5, "count": 102 },
              { "D": 6, "count": 191 },
              { "D": 7, "count": 313 },
              { "D": 8, "count": 672 },
              { "D": 1, "count": 9 },
              { "D": 3, "count": 10 },
              { "D": 9, "count": 1472 },
              { "D": 4, "count": 55 },
              { "D": 2, "count": 8 }
            ],
            "total": 2832
          },
          {
            "event_type": "speedup",
            "deciles": [
              { "D": 9, "count": 106 },
              { "D": 7, "count": 315 },
              { "D": 10, "count": 1 }
            ],
            "total": 422
          }
        ]
      },
      {
        "severity": "mass",
        "event_types": [
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 6, "count": 2 },
              { "D": 4, "count": 1 },
              { "D": 8, "count": 2 },
              { "D": 9, "count": 10 }
            ],
            "total": 15
          },
          {
            "event_type": "speedup",
            "deciles": [{ "D": 7, "count": 6 }],
            "total": 6
          },
          {
            "event_type": "brake",
            "deciles": [{ "D": 9, "count": 15 }],
            "total": 15
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 9, "count": 10 },
              { "D": 3, "count": 1 },
              { "D": 8, "count": 4 }
            ],
            "total": 15
          }
        ]
      },
      {
        "severity": "light",
        "event_types": [
          {
            "event_type": "speedup",
            "deciles": [{ "D": 7, "count": 4 }],
            "total": 4
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 9, "count": 4 },
              { "D": 7, "count": 3 },
              { "D": 8, "count": 2 },
              { "D": 2, "count": 1 },
              { "D": 6, "count": 1 }
            ],
            "total": 11
          },
          {
            "event_type": "brake",
            "deciles": [
              { "D": 8, "count": 3 },
              { "D": 9, "count": 7 },
              { "D": 7, "count": 1 }
            ],
            "total": 11
          },
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 2, "count": 1 },
              { "D": 9, "count": 5 },
              { "D": 8, "count": 3 },
              { "D": 6, "count": 1 }
            ],
            "total": 10
          }
        ]
      }
    ]
  },
  {
    "user": "pedestrian",
    "severities": [
      {
        "severity": "severe",
        "event_types": [
          {
            "event_type": "cornering_left",
            "deciles": [
              { "D": 4, "count": 30 },
              { "D": 9, "count": 97 },
              { "D": 6, "count": 30 },
              { "D": 5, "count": 12 },
              { "D": 7, "count": 60 },
              { "D": 2, "count": 23 },
              { "D": 8, "count": 76 }
            ],
            "total": 328
          },
          {
            "event_type": "cornering_right",
            "deciles": [
              { "D": 7, "count": 48 },
              { "D": 4, "count": 19 },
              { "D": 2, "count": 14 },
              { "D": 9, "count": 94 },
              { "D": 3, "count": 17 },
              { "D": 5, "count": 24 },
              { "D": 6, "count": 37 },
              { "D": 8, "count": 80 }
            ],
            "total": 333
          },
          {
            "event_type": "speedup",
            "deciles": [
              { "D": 9, "count": 3 },
              { "D": 7, "count": 30 }
            ],
            "total": 33
          },
          {
            "event_type": "brake",
            "deciles": [
              { "D": 8, "count": 107 },
              { "D": 4, "count": 4 },
              { "D": 7, "count": 55 },
              { "D": 2, "count": 1 },
              { "D": 1, "count": 1 },
              { "D": 9, "count": 121 },
              { "D": 6, "count": 43 },
              { "D": 5, "count": 16 }
            ],
            "total": 348
          }
        ]
      },
      {
        "severity": "light",
        "event_types": [
          {
            "event_type": "brake",
            "deciles": [{ "D": 7, "count": 1 }],
            "total": 1
          },
          {
            "event_type": "cornering_left",
            "deciles": [{ "D": 6, "count": 1 }],
            "total": 1
          },
          {
            "event_type": "cornering_right",
            "deciles": [{ "D": 8, "count": 1 }],
            "total": 1
          }
        ]
      }
    ]
  }
]


# Convertir los datos a un DataFrame para facilitar el trazado
def prepare_data(data, user):
    records = []
    for severity_data in data:
        if severity_data["user"] == user:
            for severity in severity_data["severities"]:
                for event in severity["event_types"]:
                    # Crear un diccionario con conteos por deciles
                    decile_counts = {f"D{decile['D']}": decile['count'] for decile in event["deciles"]}
                    # Asegurar que todos los deciles del 1 al 10 estén presentes
                    for d in range(1, 11):
                        decile_key = f"D{d}"
                        if decile_key not in decile_counts:
                            decile_counts[decile_key] = 0  # Asignar 0 si no hay conteo
                    records.append({
                        "Severity": severity["severity"],
                        "Event Type": event["event_type"],
                        **decile_counts
                    })
    return pd.DataFrame(records)

# Generar gráficos para cada usuario
for user_data in data:
    user = user_data["user"]
    df = prepare_data(data, user)

    # Trazado
    plt.figure(figsize=(10, 6))
    for i, (severity, group) in enumerate(df.groupby("Severity")):
        # Extraer columnas de deciles en orden del 1 al 10
        decile_columns = [f"D{d}" for d in range(1, 11)]
        ax = plt.subplot(1, 3, i + 1)
        # Graficar las barras apiladas
        group.set_index("Event Type")[decile_columns].plot(kind='bar', stacked=True, ax=ax)

        # Invertir el orden de la leyenda
        ax.legend(reversed(ax.get_legend_handles_labels()[0]), 
                  reversed(ax.get_legend_handles_labels()[1]), title='Deciles', bbox_to_anchor=(1, 1), loc='upper left')

        ax.set_title(f"{severity.capitalize()} Severity")
        ax.set_xlabel("Event Type")
        ax.set_ylabel("Total Events")
        plt.xticks(rotation=45)

    plt.suptitle(f"Event Totals by Severity for {user.capitalize()}")
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()