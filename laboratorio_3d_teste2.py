import threading
import time
import random
import logging
from collections import defaultdict

# =====================================================
# CONFIGURAÇÕES
# =====================================================

CAPACIDADE_FILA = 5
NUM_ESTUDANTES = 5

# Prioridades:
# 0 = Professor (maior prioridade)
# 1 = Pós-graduação
# 2 = Graduação

servidor_central = [
    (0, "PROF_001"),
    (0, "PROF_002"),
    (1, "POS_001"),
    (1, "POS_002"),
    (0, "PROF_003"),
    (2, "GRAD_001"),
    (2, "GRAD_002"),
    (1, "POS_003"),
    (2, "GRAD_003"),
    (0, "PROF_004"),
    (1, "POS_004"),
    (2, "GRAD_004"),
    (2, "GRAD_005"),
    (1, "POS_005"),
    (0, "PROF_005")
]

fila_local = []

# =====================================================
# SINCRONIZAÇÃO
# =====================================================

mutex = threading.Lock()

sem_reabastecer = threading.Semaphore(0)
sem_fila_pronta = threading.Semaphore(0)

reposicao_solicitada = False

# =====================================================
# ESTATÍSTICAS
# =====================================================

total_impressoes = 0
total_reabastecimentos = 0

impressoes_por_estudante = defaultdict(int)

# =====================================================
# LOG
# =====================================================

logging.basicConfig(
    filename="log_impressao.txt",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)

def registrar(msg):
    print(msg)
    logging.info(msg)

# =====================================================
# SERVIDOR CENTRAL
# =====================================================

def servidor():

    global reposicao_solicitada
    global total_reabastecimentos

    while True:

        sem_reabastecer.acquire()

        with mutex:

            if not servidor_central:
                reposicao_solicitada = False

                for _ in range(NUM_ESTUDANTES):
                    sem_fila_pronta.release()

                registrar(
                    "[SERVIDOR] Não existem mais arquivos pendentes."
                )
                return

            quantidade = min(
                CAPACIDADE_FILA,
                len(servidor_central)
            )

            novos = []

            for _ in range(quantidade):
                novos.append(servidor_central.pop(0))

            fila_local.extend(novos)

            # mantém a fila ordenada por prioridade
            fila_local.sort(key=lambda x: x[0])

            total_reabastecimentos += 1
            reposicao_solicitada = False

            registrar(
                f"[SERVIDOR] Reabasteceu a fila com {quantidade} arquivos."
            )

            registrar(
                f"[SERVIDOR] Arquivos restantes no servidor: "
                f"{len(servidor_central)}"
            )

            for _ in range(NUM_ESTUDANTES):
                sem_fila_pronta.release()

# =====================================================
# ESTUDANTES
# =====================================================

def estudante(identificador):

    global total_impressoes
    global reposicao_solicitada

    while True:

        arquivo = None

        with mutex:

            if len(fila_local) == 0:

                if not servidor_central and not reposicao_solicitada:
                    return

                if not reposicao_solicitada:

                    reposicao_solicitada = True

                    registrar(
                        f"[ESTUDANTE {identificador}] "
                        f"Solicitou reabastecimento."
                    )

                    sem_reabastecer.release()

            else:

                arquivo = fila_local.pop(0)

                total_impressoes += 1

                impressoes_por_estudante[
                    identificador
                ] += 1

                registrar(
                    f"[ESTUDANTE {identificador}] "
                    f"Imprimiu {arquivo[1]}"
                )

        if arquivo is None:

            sem_fila_pronta.acquire()

            with mutex:
                if not fila_local and not servidor_central:
                    return

            continue

        time.sleep(random.uniform(0.3, 1.0))

# =====================================================
# MAIN
# =====================================================

thread_servidor = threading.Thread(
    target=servidor
)

thread_servidor.start()

threads = []

for i in range(1, NUM_ESTUDANTES + 1):

    t = threading.Thread(
        target=estudante,
        args=(i,)
    )

    threads.append(t)
    t.start()

for t in threads:
    t.join()

thread_servidor.join()

# =====================================================
# RELATÓRIO FINAL
# =====================================================

if impressoes_por_estudante:

    melhor_estudante = max(
        impressoes_por_estudante,
        key=impressoes_por_estudante.get
    )

    qtd = impressoes_por_estudante[
        melhor_estudante
    ]
else:
    melhor_estudante = None
    qtd = 0

print("\n========== RELATÓRIO ==========")

print(
    f"Total de impressões realizadas: "
    f"{total_impressoes}"
)

print(
    f"Total de reabastecimentos: "
    f"{total_reabastecimentos}"
)

print(
    f"Estudante que mais imprimiu: "
    f"{melhor_estudante} ({qtd} trabalhos)"
)