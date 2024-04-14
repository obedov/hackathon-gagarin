from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score


def teams_score(true_labels, predicted_labels):
    # Расчет точности
    accuracy = accuracy_score(true_labels, predicted_labels)

    # Расчет F1-меры с усреднением по выборкам
    F1_score = f1_score(true_labels, predicted_labels, average='weighted')

    return (0.5 * F1_score + 0.5 * accuracy) * 100
