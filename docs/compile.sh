#!/usr/bin/env bash
set -e

# Compila el informe académico en PDF.
# Uso:
#   cd "$(dirname "$0")"
#   ./compile.sh
#
# Requiere que LaTeX (pdflatex) esté instalado en el sistema.

cd "$(dirname "$0")"

pdflatex -interaction=nonstopmode informe.tex
pdflatex -interaction=nonstopmode informe.tex

echo "Compilación finalizada. Salida: informe.pdf"


