# OptimiseSpotDetection
Setup spot detection for single molecule FISH, using IdentifySpots2D.m

Implemented as a GC3Pie application with command line arguments. The application accesses images on a TissueMAPS instance via the TmClient API and derives both rescaling limits and spot count per image as a function of threshold. Results are presented as a plot, similar to Fig. 5C (Stoeger, Battich et al., Methods 85: 44-53, 2015)
