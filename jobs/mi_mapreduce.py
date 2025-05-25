from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class CreditAnalysis(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--task', help='Tarea: total_monto_credito, prom_tasa_producto, total_desemb_municipio, prom_monto_rango')

    def steps(self):
        task = self.options.task
        if task == 'total_monto_credito':
            return [MRStep(mapper=self.mapper_total_monto_credito, reducer=self.reducer_sum)]
        elif task == 'prom_tasa_producto':
            return [MRStep(mapper=self.mapper_prom_tasa_producto, reducer=self.reducer_avg)]
        elif task == 'total_desemb_municipio':
            return [MRStep(mapper=self.mapper_total_desemb_municipio, reducer=self.reducer_sum)]
        elif task == 'prom_monto_rango':
            return [MRStep(mapper=self.mapper_prom_monto_rango, reducer=self.reducer_avg)]
        else:
            raise ValueError("Tarea no válida")

    # --- Utilidades ---
    def limpiar_monto(self, monto_str):
        return float(monto_str.replace('$', '').replace(',', '').strip())

    def reducer_sum(self, key, values):
        yield key, sum(values)

    def reducer_avg(self, key, values):
        total = 0
        count = 0
        for val in values:
            total += val
            count += 1
        promedio = total / count if count else 0
        yield key, round(promedio, 2)

    # --- 1. Total monto por tipo de crédito ---
    def mapper_total_monto_credito(self, _, line):
        try:
            row = next(csv.reader([line]))
            if row[0] != 'Fecha':
                tipo_credito = row[7]
                monto = self.limpiar_monto(row[10])
                yield tipo_credito, monto
        except:
            pass

    # --- 2. Promedio de tasa por producto ---
    def mapper_prom_tasa_producto(self, _, line):
        try:
            row = next(csv.reader([line]))
            if row[0] != 'Fecha':
                producto = row[9]
                tasa = float(row[11])
                yield producto, tasa
        except:
            pass

    # --- 3. Total desembolsos por municipio ---
    def mapper_total_desemb_municipio(self, _, line):
        try:
            row = next(csv.reader([line]))
            if row[0] != 'Fecha':
                municipio = row[20]
                num_desemb = int(row[13])
                yield municipio, num_desemb
        except:
            pass

    # --- 4. Promedio de monto por rango ---
    def mapper_prom_monto_rango(self, _, line):
        try:
            row = next(csv.reader([line]))
            if row[0] != 'Fecha':
                rango = row[18]
                monto = self.limpiar_monto(row[10])
                yield rango, monto
        except:
            pass

if __name__ == '__main__':
    CreditAnalysis.run()