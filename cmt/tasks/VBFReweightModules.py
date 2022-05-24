## VBF Reweighting Module
## Implements the formula from slides 11 and 12 of:
## https://indico.cern.ch/event/890825/contributions/3759797/attachments/1993521/3324873/2020_25_02_bbtautau_HIGPAG.pdf
## C -> vector of couplings
## S -> vector of samples (generated xs)
## V -> vector of components (a,b,c,iab,ibc,iac)
## M -> matrix that describes the single samples as sum of components, so that S = M * V
## so that M^-1 * S = V
##
## And the final cross section is sigma(kv, k2v, kl) = C^T * V = C^T * M-1 * S

import sympy
from sympy import *
from sympy import Matrix
from array import array
import sys
import ROOT


#########################
### General functions ###
#########################

# General function to rescale any histogram to a specific value
def scaleTo(hIn, val):
    hIn.Scale(val)

# General function to build a sympy.Matrix from a list of "inputSample"
# def build_matrix(sample_list):    
    # if len(sample_list) != 6:
        # print("[ERROR] : expecting 6 samples in input")
        # raise RuntimeError("malformed input sample list")

    # M_tofill = [
        # [None,None,None,None,None,None],
        # [None,None,None,None,None,None],
        # [None,None,None,None,None,None],
        # [None,None,None,None,None,None],
        # [None,None,None,None,None,None],
        # [None,None,None,None,None,None]
    # ]

    # # Implement the 6 scalings
    # for isample, sample in enumerate(sample_list):
        # M_tofill[isample][0] = sample.val_kv**2 * sample.val_kl**2
        # M_tofill[isample][1] = sample.val_kv**4
        # M_tofill[isample][2] = sample.val_k2v**2
        # M_tofill[isample][3] = sample.val_kv**3 * sample.val_kl
        # M_tofill[isample][4] = sample.val_kv    * sample.val_k2v    * sample.val_kl
        # M_tofill[isample][5] = sample.val_kv**2 * sample.val_k2v
    
    # res = Matrix(M_tofill)
    # return res


################################################
### Class for handling the six input samples ###
################################################
class InputSample:
    def __init__(self, val_kv, val_k2v, val_kl, val_xs):
        self.kv  = val_kv
        self.k2v = val_k2v
        self.kl  = val_kl
        self.xs  = val_xs

########################################
### Class for actual VBF reweighting ###
########################################
class VBFReweight:
    def __init__(self, samples):
        self.n_samples = len(samples)
        self.sample_list = samples
        self.M = sympy.Matrix([
            [
                sample.kv**2 * sample.kl**2,
                sample.kv**4,
                sample.k2v**2,
                sample.kv**3 * sample.kl,
                sample.kv * sample.k2v * sample.kl,
                sample.kv**2 * sample.k2v,
            ]
            for i, sample in enumerate(samples)
        ])

        # the vector of couplings
        kv, k2v, kl = sympy.symbols("kv k2v kl")
        c = sympy.Matrix([
            [kv**2 * kl**2],
            [kv**4],
            [k2v**2],
            [kv**3 * kl],
            [kv * k2v * kl],
            [kv**2 * k2v],
        ])

        self.kv = kv
        self.k2v = k2v
        self.kl = kl

        # the vector of symbolic sample cross sections
        s = sympy.Matrix([
            [sympy.Symbol("xs{}".format(i))]
            for i in range(self.n_samples)
        ])

        self.xs0 = s[0]
        self.xs1 = s[1]
        self.xs2 = s[2]
        self.xs3 = s[3]
        self.xs4 = s[4]
        self.xs5 = s[5]
        self.xs6 = s[6]

        # actual computation, i.e., matrix inversion and multiplications with vectors
        M_inv = self.M.pinv()
        self.coeffs = c.transpose() * M_inv
        self.sigma = self.coeffs * s

    # Returns the modeled cross section and histogram given the target couplings
    def modelSignal(self, t_kv, t_k2v, t_kl, histos, target_xs=-1.):
        # Eval the final cross section
        total_xs = self.sigma[0].evalf(subs={
            self.kv  : t_kv,
            self.k2v : t_k2v,
            self.kl  : t_kl,
            self.xs0  : self.sample_list[0].xs,
            self.xs1  : self.sample_list[1].xs,
            self.xs2  : self.sample_list[2].xs,
            self.xs3  : self.sample_list[3].xs,
            self.xs4  : self.sample_list[4].xs,
            self.xs5  : self.sample_list[5].xs,
            self.xs6  : self.sample_list[6].xs,
        })

        # Eval the single coefficients
        eval_coeffs = []
        for coeff in self.coeffs:
            eval_coeffs.append(coeff.evalf(subs={self.kv:t_kv, self.k2v:t_k2v, self.kl:t_kl}))

        # Get a clone of the input histograms to be scaled and summed
        hists = []
        for histo in histos:
            hists.append(histo.Clone())

        # Normalize each histo to his coefficient * xs
        for i,sample in enumerate(self.sample_list):
            if target_xs == -1.:
                scaleTo(hists[i], eval_coeffs[i])
            else:
                scaleTo(hists[i], eval_coeffs[i] * target_xs)
                #scaleTo(hists[i], eval_coeffs[i] * target_xs / sample.val_xs)

        # Get the final histogram by adding the six hists
        for i,histo in enumerate(hists):
            if i == 0:
                tot_histo = histo.Clone('VBF_kv_'+str(t_kv)+'_k2v_'+str(t_k2v)+'_C3_'+str(t_kl))
            else:
                tot_histo.Add(histo)

        return total_xs, tot_histo
