pdflatex report.tex
bibtex report
pdflatex report.tex
pdflatex report.tex

rm *.aux
rm *.bbl
rm *.toc
rm *.log
rm *.blg
rm *.out

mv report.pdf ds-westra-harmsma.pdf