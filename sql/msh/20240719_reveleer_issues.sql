-- chases they say are new that we sent already
SELECT pqm.*
FROM
--     reveleer_chase_file_details WHERE chase_id in (
    reveleer_chases c
join fdw_member_doc.qm_patient_measures pqm on pqm.id = any(c.qm_patient_measure_ids)
WHERE c.id in (
  386958
, 386573
, 520427
, 520088
, 517595
, 383506
, 516460
, 382134
, 642075
, 642063
, 515844
, 514791
, 640940
, 640867
, 640861
, 640827
, 640783
, 640550
, 380450
, 639631
, 639626
, 512776
, 639005
, 638819
, 638699
, 638654
, 638641
, 638632
, 638625
, 638582
, 638577
, 638569
, 511183
, 509312
, 635717
, 508766
, 508289
, 508269
, 507343
, 633694
, 373385
, 633676
, 633667
, 373350
, 373326
, 371920
, 371300
, 369953
, 369817
, 369637
, 368524
, 281629
, 367562
, 500356
, 500351
, 500071
, 499809
, 365893
, 365057
, 365045
, 365031
, 758410
, 364942
, 364911
, 497382
, 399807
, 362190
, 361060
, 361057
, 493760
, 493662
, 493600
, 618998
, 618992
, 357136
, 399569
, 408238
, 353502
, 353469
, 614367
, 614124
, 352622
, 352620
, 352610
, 352566
, 745695
, 352540
, 614027
, 613973
, 613957
, 745612
, 613906
, 485379
, 613257
, 613108
, 612944
, 612639
, 612543
, 612402
, 612360
, 612338
, 612320
, 612287
, 612212
, 612205
, 612079
, 612004
, 611925
, 611877
, 611688
, 611552
, 611391
, 610938
, 610896
, 610876
, 610866
, 610864
, 610787
, 610483
, 476661
, 476415
, 344085
, 343479
, 475330
, 343195
, 343079
, 342518
, 342489
, 602967
, 733567
, 602882
, 602840
, 602833
, 602759
, 602746
, 602719
, 602520
, 602446
, 602434
, 471488
, 470961
, 470921
, 338529
, 470468
, 336065
, 336054
, 336048
, 467553
, 335686
, 335568
, 596597
, 467299
, 467276
, 467213
, 466821
, 465625
, 333759
, 333757
, 333742
, 333727
, 331965
, 331916
, 592712
, 462259
, 586280
, 456300
, 456112
, 585660
, 323595
, 320302
, 451079
, 319427
, 450126
, 318505
, 449529
, 579922
, 579612
, 579482
, 315981
, 447465
, 315137
, 315125
, 446599
, 445688
, 313896
, 445355
, 445280
, 313765
, 313713
, 443605
, 443578
, 443550
, 312081
, 312068
, 443464
, 443279
, 443213
, 443160
, 443017
, 442862
, 311334
, 442446
, 442362
, 442278
, 441892
, 441876
, 310496
, 310466
, 441744
, 441492
, 441336
, 309974
, 441277
, 309841
, 441105
, 441049
, 309480
, 309463
, 440678
, 440500
, 309085
, 440331
, 308958
, 440275
, 440244
, 308912
, 440209
, 439747
, 439610
, 439509
, 308153
, 698953
, 439352
, 308016
, 439305
, 439296
, 307573
, 307555
, 438858
, 569910
, 438741
, 438689
, 438528
, 307113
, 306777
, 437940
, 437728
, 437668
, 437633
, 306163
, 437354
, 305933
, 305655
, 436859
, 436826
, 436819
, 436816
, 436758
, 436570
, 436540
, 305265
, 436378
, 436304
, 435915
, 559252
, 558681
, 660672
, 660649
, 660647
, 660644
, 535457
, 660584
, 399279
, 399273
, 399260
, 399236
, 399234
, 399225
, 399195
, 399178
, 399168
, 399140
, 399119
, 399114
, 399111
, 399109
, 399108
, 399103
, 659937
, 659925
, 398498
, 659110
, 659095
, 659093
, 659092
, 658848
, 397615
, 422810
, 395096
, 395087
, 530254
, 399394
, 528059
, 391010
, 390929
, 390887
, 389593
, 389430
, 649233
, 389254
, 389251
, 389248
, 389231
, 523440
, 523438
, 523433
, 523430
, 523425
    )
-- and c.measure_code != 'COA'
;
SELECT a.patient_id, a.attribute_code, a.attribute_value, h.test_date, h.value, h.source, h.inserted_at, h.document_id, h.is_compliant_result, h.is_incomplete_result
FROM
    analytics.public.reveleer_attribute_file_details a
join fdw_member_doc.patient_hba1cs h on h.patient_id = a.patient_id
WHERE
    sample_id::int IN (
                  1219919, 1221863, 1223164, 1225343, 1225694, 1227307, 1232962, 1233139, 1234178, 1234473, 1235358,
                  1237267, 1237341, 1263916, 1266557, 1267027, 1275422, 1280792, 1282637, 1297472, 1303679, 1305137,
                  1306573, 1306996, 1312612, 1313885, 1314721, 1326246, 1347767
        )
and reveleer_file_id = 5056
order by a.patient_id, h.test_date
    ;

SELECT distinct rc.*, rp.name
FROM
    reveleer_chases rc
join reveleer_compliance_file_details c on rc.id::text = c.sample_id
join reveleer_projects rp on rp.id = rc.reveleer_project_id
where rc.measure_code = 'A1C9'
and rc.yr = 2024
;
SELECT *
FROM
    reveleer_compliance_file_details
WHERE measure_id = 'A1C9';