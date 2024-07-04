
library(profvis)

profvis(Means_pipeline_sac(cache_inventory, 
                                    cache_sac, 
                                    dl_aux))

profvis(Means_pipeline_tar(cache_inventory, 
                           cache, 
                           dl_aux))
